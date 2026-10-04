// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

package server

import (
	"errors"
	"strings"
	"sync"
	"time"
)

type channel struct {
	done    chan struct{} // empty-channel worker has completed teardown
	name    string
	members []channelMember

	// messages receives messages to be broadcast over the channel.
	messages chan channelMessage
	// joins receives members to add to the channel
	joins chan joinChannelRequest
	// parts receives member IDs to remove from the channel
	// If there are no more members, and no pending joins, the channel will be destroyed.
	parts chan leaveChannelRequest

	pendingJoinsLock sync.Mutex // Protects pendingJoins
	// pendingJoins is the number of clients who have fetched this channel from the registry, but have not yet joined
	pendingJoins int
}

type channelMember struct {
	client         *client // nil for in-process synthetic members
	id             uint64
	connectionType string
	events         chan<- Message
	stop           func(string)
}

// enqueue preserves FIFO delivery while the client can keep up. Once the
// bounded queue is full, terminate the connection rather than silently omit a
// protocol message and leave the peer running with incomplete protocol state.
// Called without registry or pendingJoins locks; stop may close network I/O.
func (member channelMember) enqueue(msg Message) {
	select {
	case member.events <- msg:
	default:
		member.stop("Client event queue overflow")
	}
}

type joinChannelRequest struct {
	member channelMember
	resp   chan interface{} // response could either be a list of existing members or an error
}

// joinChannel adds a member to the named channel, creating it if it doesn't already exist.
func joinChannel(name string, member channelMember, reg *registry) (*channel, []channelMember, error) {
	reg.lock.Lock()
	if _, exists := reg.clients[member.id]; exists {
		reg.lock.Unlock()
		return nil, nil, errors.New("already a member")
	}
	if member.client != nil && reg.capacity != nil {
		c := member.client
		if !reg.capacity.beginAdmission(c) {
			reg.lock.Unlock()
			return nil, nil, errAdmissionExpired
		}
		overlap, reason := reg.capacity.admit(c, name, member.connectionType, time.Now())
		if reason != "" {
			reg.lock.Unlock()
			if reason == capacityReasonTimedOut || reason == capacityReasonBadState {
				return nil, nil, errAdmissionExpired
			}
			return nil, nil, errCapacity
		}
		c.overlap = overlap
		if overlap {
			c.overlapStart <- reg.capacity.overlapDeadline(c)
		}
		c.finishStartup()
	}
	reg.clients[member.id] = member
	if len(reg.clients) > reg.maxClients {
		reg.maxClients = len(reg.clients)
		reg.maxClientsTime = time.Now()
	}

	c, ok := reg.channels[name]
	if !ok {
		c = &channel{
			done:     make(chan struct{}),
			name:     name,
			members:  []channelMember{},
			messages: make(chan channelMessage),
			joins:    make(chan joinChannelRequest),
			parts:    make(chan leaveChannelRequest),
		}
		reg.channels[name] = c
		reg.workers.Add(1)
		go func() {
			defer close(c.done)
			defer reg.workers.Done()
			c.start(reg)
		}()

		if c.isE2e() {
			reg.numE2eChannels++
		}
		if len(reg.channels) > reg.maxChannels {
			reg.maxChannels = len(reg.channels)
			reg.maxChannelsTime = time.Now()
		}
	}

	// We don't want to join the channel while the registry is locked, because slow channel goroutines will bog it down for everyone.
	// But we do need to note that there is a join pending, so that if the channel becomes empty before this member joins,
	// it doesn't spin down and remove itself from the registry.
	c.pendingJoinsLock.Lock()
	c.pendingJoins++
	c.pendingJoinsLock.Unlock()
	reg.lock.Unlock()
	// Join the channel, now that the registry is unlocked
	req := joinChannelRequest{
		member: member,
		resp:   make(chan interface{}),
	}
	c.joins <- req

	switch result := (<-req.resp).(type) {
	case error:
		return c, nil, result
	case []channelMember:
		if member.client != nil {
			reg.capacity.markSuccessful(member.client)
		}
		return c, result, nil
	}

	return c, nil, errors.New("Received unknown type from channel")
}

type leaveChannelRequest struct {
	id   uint64
	resp chan bool // true when the empty worker will exit
}

// leave removes a member from the channel, destroying the channel if it is empty.
// leave waits for channel and registry removal, including empty-channel cleanup.
// No channel delivery or registry ping dispatch can target id after it returns.
// The caller must keep receiving events until then to unblock in-flight sends.
func (c *channel) leave(id uint64) {
	req := leaveChannelRequest{
		id:   id,
		resp: make(chan bool),
	}
	c.parts <- req
	if empty := <-req.resp; empty {
		<-c.done
	}
}

func (c *channel) start(reg *registry) {
	for {
		select {
		case req := <-c.joins:
			var exists bool
			for _, member := range c.members {
				if req.member.id == member.id {
					exists = true
					break // Already in the channel
				}
			}

			if !exists {
				// Send current members to the joiner
				// and notify existing members.
				// The caller converts this snapshot while membership can change.
				req.resp <- append([]channelMember(nil), c.members...)
				c.broadcast(joinedChannelMSG(req.member))
				c.members = append(c.members, req.member)
			} else {
				req.resp <- errors.New("already a member")
			}
			c.pendingJoinsLock.Lock()
			c.pendingJoins--
			c.pendingJoinsLock.Unlock()

		case req := <-c.parts:
			for i, member := range c.members {
				if req.id == member.id {
					copy(c.members[i:], c.members[i+1:])
					// stop is a method value holding the departed *client. Clear
					// the unused slot so a live channel cannot retain that client.
					c.members[len(c.members)-1] = channelMember{}
					c.members = c.members[:len(c.members)-1]
					c.broadcast(leftChannelMSG(member))
					break
				}
			}
			reg.lock.Lock()
			delete(reg.clients, req.id)
			// Destroy the channel if there are no more members and no more pending joins
			c.pendingJoinsLock.Lock()
			empty := len(c.members) == 0 && c.pendingJoins == 0
			if empty {
				delete(reg.channels, c.name)
				if c.isE2e() {
					reg.numE2eChannels--
				}
			}
			c.pendingJoinsLock.Unlock()
			reg.lock.Unlock()

			// Acknowledge only after all membership and registry cleanup.
			req.resp <- empty
			if empty {
				return
			}

		case msg := <-c.messages:
			for _, member := range c.members {
				if msg.origin != member.id {
					member.enqueue(msg)
				}
			}

		}
	}
}

func (c *channel) broadcast(msg Message) {
	for _, member := range c.members {
		member.enqueue(msg)
	}
}

func (c *channel) isE2e() bool {
	return strings.HasPrefix(c.name, "E2E_") && len(c.name) == 68
}

type joinedChannelMSG channelMember

func (joinedChannelMSG) Name() string {
	return "joined_channel"
}

type leftChannelMSG channelMember

func (leftChannelMSG) Name() string {
	return "left_channel"
}

type channelMessage struct {
	origin uint64
	msg    map[string]interface{}
}

func (channelMessage) Name() string {
	return "channel_message"
}
