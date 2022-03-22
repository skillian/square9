package web

import (
	"sync"

	"github.com/skillian/errors"
)

// SessionPool is a pool of individual sessions.  Sessions are not safe for
// concurrent use but SessionPools are.
//
// Implementation notes:
//
// The implementation is too complicated but I don't yet know what to do about
// it.  I'm open to suggestions!
//
// The sessions field is initialized with a capacity equal to the limit value
// passed to NewSessionPool.  The limit field also gets this capacity.  No
// sessions are created initialy; not until the first call to Session is made.
// There, we try to get from the sessions channel (and fail the first time)
// so we create a new session.  If using the session succeeds, it is put into
// the channel so a future attempt through the "fast path" (receive from the
// sessions channel) succeeds.  If usage of the session returns an error
type SessionPool struct {
	// mutex is only held if we were unable to get a session from
	mutex    sync.Mutex
	sessions chan *Session
	limit    int
	factory  func() (*Session, error)
}

var _ Client = (*SessionPool)(nil)

// NewSessionPool creates a pool of clients.
func NewSessionPool(limit int, factory func() (*Session, error)) *SessionPool {
	if limit <= 0 {
		panic("limit cannot be less than or equal to 0")
	}
	p := &SessionPool{}
	// TODO: Do I need to do this for a memory barrier?
	p.mutex.Lock()
	p.sessions = make(chan *Session, limit)
	p.limit = limit
	p.factory = factory
	p.mutex.Unlock()
	return p
}

func (p *SessionPool) getSession() (*Session, error) {
	sessionOrErr := func(s *Session, ok bool) (*Session, error) {
		if !ok {
			return nil, errors.Errorf("Client pool was closed.")
		}
		return s, nil
	}
	select {
	case s, ok := <-p.sessions:
		return sessionOrErr(s, ok)
	default:
	}
	p.mutex.Lock()
	// Maybe someone put one back while we were waiting for the lock:
	select {
	case s, ok := <-p.sessions:
		p.mutex.Unlock()
		return sessionOrErr(s, ok)
	default:
	}
	// Maybe we can create a new one with the factory:
	if p.limit > 0 {
		p.limit--
		// if the factory directly or indirectly touches the
		// SessionPool (e.g. calling Close in a panic), we'll have a
		// deadlock if we don't unlock here.
		p.mutex.Unlock()
		s, err := p.factory()
		if err != nil {
			p.mutex.Lock()
			p.limit++
			p.mutex.Unlock()
			return nil, err
		}
		return s, nil
	}
	// There are no clients available and no slots left in the pool, so
	// we have to wait for one:
	p.mutex.Unlock()
	s, ok := <-p.sessions
	return sessionOrErr(s, ok)
}

// Session borrows a session from the pool to perform some operation.
func (p *SessionPool) Session(f func(s *Session) error) error {
	defer func() {
		if v := recover(); v != nil {
			p.mutex.Lock()
			p.limit++
			p.mutex.Unlock()
			panic(v)
		}
	}()
	var errs []error
	for tries := 0; tries < 2; tries++ {
		s, err := p.getSession()
		if err != nil {
			errs = append(errs, err)
			break
		}
		err = f(s)
		if err == nil {
			p.sessions <- s
			return nil
		}
		errs = append(errs, err)
		if err = s.Close(); err != nil {
			errs = append(errs, err)
		}
		p.mutex.Lock()
		// we're going to discard this session, so we increase the limit
		// so that a future call to getSession can "reclaim" the dropped
		// session.
		p.limit++
		p.mutex.Unlock()
	}
	if len(errs) > 1 {
		for _, err := range errs[1:] {
			errs[0] = errors.CreateError(err, nil, errs[0], 0)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Close the pool by waiting for and closing all the sessions from the pool.
func (p *SessionPool) Close() error {
	p.mutex.Lock()
	p.factory = func() (*Session, error) { return nil, errors.Errorf("pool is closing") }
	limit := p.limit
	p.limit = 0
	p.mutex.Unlock()
	var errs []error
	for count := cap(p.sessions) - limit; count > 0; count-- {
		c := <-p.sessions
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	close(p.sessions)
	var err error
	for _, err2 := range errs {
		err = errors.CreateError(err2, nil, err, 0)
	}
	return err
}

// RWClient maintains two client implementations: one for read-only access
// and another for read-write access.  By default, when implementing the Client
// interface, the full-access implementation is used.
type RWClient struct {
	readOnly   Client
	fullAccess Client
	defaulter  func(c *RWClient) Client
}

// NewRWClient implements the Client interface by choosing between a read-only
// implementation or a read-write implementation.  By default, when the RWClient
// implements Client directly, it uses the full access client.  This can be
// changed with the defaultReadOnly parameter here.
func NewRWClient(readOnly, fullAccess Client, defaultReadOnly bool) *RWClient {
	defaulter := (*RWClient).FullAccess
	if defaultReadOnly {
		defaulter = (*RWClient).ReadOnly
	}
	return &RWClient{
		readOnly:   readOnly,
		fullAccess: fullAccess,
		defaulter:  defaulter,
	}
}

// FullAccess gets the FullAccess client explicitly to implement the Client
// interface.
func (c *RWClient) FullAccess() Client { return c.fullAccess }

// ReadOnly gets the read-only client to implement the Client interface.
func (c *RWClient) ReadOnly() Client { return c.readOnly }

// Session implements the Client interface by getting the FullAccess or ReadOnly
// client (depending on how NewRWClient was called) and delegating to that
// implementation.
func (c *RWClient) Session(f func(s *Session) error) error {
	return c.defaulter(c).Session(f)
}
