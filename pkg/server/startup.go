package server

import "time"

// watchStartup owns an absolute post-handshake deadline. Admission and timeout
// arbitrate under capacity.mu; stop never releases the physical lifecycle slot.
func (c *client) watchStartup(done chan<- struct{}) {
	defer close(done)
	timer := time.NewTimer(time.Until(c.capacity.startupDeadline(c)))
	defer timer.Stop()
	select {
	case <-c.startupDone:
	case <-timer.C:
		if c.capacity.timeout(c) {
			c.stop("Startup admission timeout")
		}
	}
}

func (c *client) finishStartup() {
	if c.startupDone != nil {
		c.startupOnce.Do(func() { close(c.startupDone) })
	}
}

func (c *client) watchOverlap(done chan<- struct{}) {
	defer close(done)
	var deadline time.Time
	select {
	case deadline = <-c.overlapStart:
	case <-c.lifecycleDone:
		return
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case now := <-timer.C:
		if c.capacity.overlapValid(c, now) {
			c.stop("Configured recovery overlap expired")
		}
	case <-c.lifecycleDone:
	}
}
