package server

import "time"

const capacityAnnouncement = "Server is at capacity. Existing remote sessions are protected. Please try again later."

// rejectionSchedule is independent of I/O. The minimum spacing is one quarter
// of the periodic interval. Inputs coalesce in one bit; neither inputs nor
// announcements move the absolute deadline. Periodic eligibility stays anchored.
type rejectionSchedule struct {
	deadline, periodic, last time.Time
	interval, minimum        time.Duration
	input                    bool
}

func newRejectionSchedule(now time.Time, window, interval time.Duration) rejectionSchedule {
	return rejectionSchedule{deadline: now.Add(window), periodic: now.Add(interval), last: now, interval: interval, minimum: interval / 4}
}

func (s *rejectionSchedule) next(now time.Time) time.Duration {
	next := s.periodic
	if s.input && s.last.Add(s.minimum).Before(next) {
		next = s.last.Add(s.minimum)
	}
	if next.Before(s.last.Add(s.minimum)) {
		next = s.last.Add(s.minimum)
	}
	if s.deadline.Before(next) {
		next = s.deadline
	}
	if next.Before(now) {
		return 0
	}
	return next.Sub(now)
}

func (s *rejectionSchedule) announce(now time.Time) bool {
	if !now.Before(s.deadline) || now.Before(s.last.Add(s.minimum)) {
		return false
	}
	if now.Before(s.periodic) && !s.input {
		return false
	}
	if !now.Before(s.periodic) {
		s.periodic = s.periodic.Add(s.interval)
	}
	s.last, s.input = now, false
	return true
}

func (c *client) beginRejection(role string) {
	c.capacity.rejecting(c)
	c.finishStartup()
	c.rejected.Store(true)
	c.rejectionRole = role
	c.rejection = newRejectionSchedule(time.Now(), c.capacity.config.RejectionWindow, c.capacity.config.RejectionInterval)
	c.sendMTX.Lock()
	c.writeUntil = c.rejection.deadline
	c.sendMTX.Unlock()
	// Error is useful to logs/tools, but controllers also need actual speech.
	if role == "master" {
		c.announceCapacity()
	}
	c.sendError("server at capacity")
}

func (c *client) announceCapacity() {
	if c.rejectionRole == "master" {
		c.send(ClientResponse{"type": "speak", "sequence": []string{capacityAnnouncement}})
	}
}

// Rejected bytes are drained into fixed storage without parsing or handlers.
// A bounded mailbox represents all user input, including malformed JSON and
// whitespace. No channel membership or protocol-version path is reachable.
func (c *client) noteRejectedInput() {
	select {
	case c.rejectionInput <- struct{}{}:
	default:
	}
}
