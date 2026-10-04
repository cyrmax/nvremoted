package server

import "testing"

func TestBackpressureMemberRemovalClearsTail(t *testing.T) {
	for _, departedID := range []uint64{7, 8, 9} {
		t.Run(map[uint64]string{7: "first", 8: "middle", 9: "last"}[departedID], func(t *testing.T) {
			reg := newLifecycleRegistry()
			clients := []*client{
				joinLifecycleClient(t, reg, "test", 7),
				joinLifecycleClient(t, reg, "test", 8),
				joinLifecycleClient(t, reg, "test", 9),
			}
			ch := clients[0].channel
			ch.leave(departedID)
			defer func() {
				for _, c := range clients {
					if c.id != departedID {
						ch.leave(c.id)
					}
				}
			}()
			// The acknowledgement is a barrier after the channel's mutation.
			// There are no pending joins or other membership writers.
			want := make([]uint64, 0, 2)
			for _, c := range clients {
				if c.id != departedID {
					want = append(want, c.id)
				}
			}
			if len(ch.members) != len(want) {
				t.Fatalf("members = %d, want %d", len(ch.members), len(want))
			}
			for i, id := range want {
				if ch.members[i].id != id || ch.members[i].events == nil || ch.members[i].stop == nil {
					t.Fatalf("remaining member %d = %#v, want client %d", i, ch.members[i], id)
				}
			}
			// Check the actual backing array, without relying on GC timing or
			// finalizers: no removed slot may retain a queue or stop method value.
			for i, member := range ch.members[:cap(ch.members)][len(ch.members):] {
				if member.id != 0 || member.connectionType != "" || member.events != nil || member.stop != nil {
					t.Fatalf("unused backing slot %d retains member: %#v", i, member)
				}
			}
		})
	}
}

func TestBackpressureJoinSnapshotSurvivesRemoval(t *testing.T) {
	reg := newLifecycleRegistry()
	first := joinLifecycleClient(t, reg, "test", 7)
	joinLifecycleClient(t, reg, "test", 8)
	joinLifecycleClient(t, reg, "test", 9)
	ch, snapshot, err := joinChannel("test", channelMember{
		id: 10, connectionType: "master", events: make(chan Message, defaultEventQueueSize),
		stop: func(string) { t.Error("unexpected overflow") },
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	ch.leave(9)
	defer func() {
		for _, id := range []uint64{7, 8, 10} {
			first.channel.leave(id)
		}
	}()
	// A join caller may still be converting the returned members while the
	// channel removes a member and clears its tail. The snapshot must own its
	// storage, including the order and references of the members it observed.
	if len(snapshot) != 3 {
		t.Fatalf("snapshot length = %d, want 3", len(snapshot))
	}
	for i, id := range []uint64{7, 8, 9} {
		if snapshot[i].id != id || snapshot[i].events == nil || snapshot[i].stop == nil {
			t.Fatalf("snapshot member %d = %#v, want client %d", i, snapshot[i], id)
		}
	}
}
