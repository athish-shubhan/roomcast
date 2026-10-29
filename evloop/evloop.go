// Package evloop is a minimal epoll event loop for many mostly idle TCP
// connections, in the style of Netpoll and gnet: one goroutine per loop
// instead of one (or two) per connection, non-blocking sockets, and buffers
// that exist only while a connection has something pending. Level-triggered
// epoll; EPOLLOUT is armed only while a connection has unwritten bytes.
//
// Everything about a Conn happens on its loop's goroutine. Other goroutines
// hand work to a loop with Submit.
package evloop

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

type Handler interface {
	OnOpen(c *Conn)
	OnData(c *Conn, p []byte) // p is only valid during the call
	OnWritable(c *Conn)       // all pending output was written
	OnClose(c *Conn, err error)
}

type Loop struct {
	ID    int
	epfd  int
	wakeR int
	wakeW int
	lfd   int
	h     Handler
	rbuf  []byte

	mu     sync.Mutex
	tasks  []func()
	spare  []func()
	woken  atomic.Bool
	conns  map[int]*Conn
	NConns atomic.Int64

	// SndBuf, if set, is applied to accepted sockets (SO_SNDBUF).
	SndBuf int

	// AfterTasks runs on the loop goroutine after each batch of submitted
	// tasks: the place to flush work the tasks accumulated (write coalescing).
	AfterTasks func()
}

type Conn struct {
	Fd         int
	L          *Loop
	Ctx        any
	out        []byte
	epOut      bool
	closed     bool
	connecting bool
}

const (
	epIn  = syscall.EPOLLIN | syscall.EPOLLRDHUP
	epOut = syscall.EPOLLOUT
)

func New(id int, h Handler, readBuf int) (*Loop, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return nil, err
	}
	l := &Loop{ID: id, epfd: epfd, wakeR: p[0], wakeW: p[1], lfd: -1, h: h, rbuf: make([]byte, readBuf), conns: map[int]*Conn{}}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, p[0], &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(p[0])}); err != nil {
		return nil, err
	}
	return l, nil
}

func sockaddr(addr string) (syscall.Sockaddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	pn, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	sa := &syscall.SockaddrInet4{Port: pn}
	if host != "" {
		ip := net.ParseIP(host).To4()
		if ip == nil {
			return nil, errors.New("evloop: IPv4 only: " + host)
		}
		copy(sa.Addr[:], ip)
	}
	return sa, nil
}

// Listen opens a listening socket owned by this loop. With SO_REUSEPORT,
// several loops can listen on the same port and the kernel spreads accepts.
func (l *Loop) Listen(addr string, backlog int) error {
	sa, err := sockaddr(addr)
	if err != nil {
		return err
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1)
	if err := syscall.Bind(fd, sa); err != nil {
		syscall.Close(fd)
		return err
	}
	if err := syscall.Listen(fd, backlog); err != nil {
		syscall.Close(fd)
		return err
	}
	l.lfd = fd
	return syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)})
}

// Dial starts a non-blocking connect; OnOpen runs when it completes. laddr
// (IP only) picks the source address, so one load generator can open more
// connections than one IP's ephemeral port range. Loop goroutine only.
func (l *Loop) Dial(addr, laddr string, ctx any) (*Conn, error) {
	sa, err := sockaddr(addr)
	if err != nil {
		return nil, err
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if laddr != "" {
		la, err := sockaddr(net.JoinHostPort(laddr, "0"))
		if err == nil {
			// IP_BIND_ADDRESS_NO_PORT: pick the port at connect time, so ports
			// are unique per 4-tuple rather than per source IP
			syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, 24, 1)
			if err := syscall.Bind(fd, la); err != nil {
				syscall.Close(fd)
				return nil, err
			}
		}
	}
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	if err := syscall.Connect(fd, sa); err != nil && err != syscall.EINPROGRESS {
		syscall.Close(fd)
		return nil, err
	}
	c := &Conn{Fd: fd, L: l, Ctx: ctx, connecting: true, epOut: true}
	l.conns[fd] = c
	l.NConns.Add(1)
	if err := syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{Events: epIn | epOut, Fd: int32(fd)}); err != nil {
		delete(l.conns, fd)
		l.NConns.Add(-1)
		syscall.Close(fd)
		return nil, err
	}
	return c, nil
}

// Submit runs f on the loop goroutine. Safe from any goroutine.
func (l *Loop) Submit(f func()) {
	l.mu.Lock()
	l.tasks = append(l.tasks, f)
	l.mu.Unlock()
	if l.woken.CompareAndSwap(false, true) {
		syscall.Write(l.wakeW, []byte{1})
	}
}

func (l *Loop) runTasks() {
	var b [64]byte
	for {
		if _, err := syscall.Read(l.wakeR, b[:]); err != nil {
			break
		}
	}
	l.woken.Store(false)
	l.mu.Lock()
	t := l.tasks
	l.tasks = l.spare[:0]
	l.mu.Unlock()
	for i, f := range t {
		f()
		t[i] = nil
	}
	l.spare = t
	if l.AfterTasks != nil {
		l.AfterTasks()
	}
}

// Run blocks, serving events until the process exits.
func (l *Loop) Run() error {
	events := make([]syscall.EpollEvent, 512)
	for {
		n, err := syscall.EpollWait(l.epfd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		for i := 0; i < n; i++ {
			fd := int(events[i].Fd)
			ev := events[i].Events
			switch fd {
			case l.wakeR:
				l.runTasks()
			case l.lfd:
				l.accept()
			default:
				c := l.conns[fd]
				if c == nil {
					continue
				}
				if c.connecting {
					if ev&(syscall.EPOLLOUT|syscall.EPOLLERR|syscall.EPOLLHUP) != 0 {
						l.finishConnect(c)
					}
					continue
				}
				if ev&syscall.EPOLLIN != 0 {
					l.read(c)
				}
				if c.closed {
					continue
				}
				if ev&(syscall.EPOLLERR|syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0 && ev&syscall.EPOLLIN == 0 {
					c.closeErr(errors.New("hangup"))
					continue
				}
				if ev&syscall.EPOLLOUT != 0 {
					c.flush()
				}
			}
		}
	}
}

func (l *Loop) accept() {
	for {
		nfd, _, err := syscall.Accept4(l.lfd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if err != nil {
			return // EAGAIN, or an error the next readiness will repeat
		}
		syscall.SetsockoptInt(nfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		if l.SndBuf > 0 {
			syscall.SetsockoptInt(nfd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, l.SndBuf)
		}
		c := &Conn{Fd: nfd, L: l}
		if err := syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, nfd, &syscall.EpollEvent{Events: epIn, Fd: int32(nfd)}); err != nil {
			syscall.Close(nfd)
			continue
		}
		l.conns[nfd] = c
		l.NConns.Add(1)
		l.h.OnOpen(c)
	}
}

func (l *Loop) finishConnect(c *Conn) {
	v, err := syscall.GetsockoptInt(c.Fd, syscall.SOL_SOCKET, syscall.SO_ERROR)
	if err == nil && v != 0 {
		err = syscall.Errno(v)
	}
	if err != nil {
		c.closeErr(err)
		return
	}
	c.connecting = false
	c.epOut = false
	syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_MOD, c.Fd, &syscall.EpollEvent{Events: epIn, Fd: int32(c.Fd)})
	l.h.OnOpen(c)
}

func (l *Loop) read(c *Conn) {
	n, err := syscall.Read(c.Fd, l.rbuf)
	if n > 0 {
		l.h.OnData(c, l.rbuf[:n])
		return
	}
	if err == syscall.EAGAIN || err == syscall.EINTR {
		return
	}
	if err == nil {
		err = errors.New("EOF")
	}
	c.closeErr(err)
}

// Write sends p now if the socket takes it, and keeps the rest until the
// socket is writable again. Loop goroutine only.
func (c *Conn) Write(p []byte) {
	if c.closed || len(p) == 0 {
		return
	}
	if len(c.out) > 0 || c.connecting {
		c.out = append(c.out, p...)
		return
	}
	n, err := syscall.Write(c.Fd, p)
	if err != nil {
		if err != syscall.EAGAIN {
			c.closeErr(err)
			return
		}
		n = 0
	}
	if n < len(p) {
		c.out = append(c.out[:0], p[n:]...)
		c.armOut(true)
	}
}

// Pending is the number of bytes the socket has not accepted yet.
func (c *Conn) Pending() int { return len(c.out) }

func (c *Conn) armOut(on bool) {
	if c.epOut == on || c.closed {
		return
	}
	c.epOut = on
	ev := uint32(epIn)
	if on {
		ev |= epOut
	}
	syscall.EpollCtl(c.L.epfd, syscall.EPOLL_CTL_MOD, c.Fd, &syscall.EpollEvent{Events: ev, Fd: int32(c.Fd)})
}

func (c *Conn) flush() {
	for len(c.out) > 0 {
		n, err := syscall.Write(c.Fd, c.out)
		if err != nil {
			if err == syscall.EAGAIN {
				return
			}
			c.closeErr(err)
			return
		}
		c.out = c.out[n:]
	}
	c.out = nil // release the buffer: idle connections hold none
	c.armOut(false)
	c.L.h.OnWritable(c)
}

func (c *Conn) Close() { c.closeErr(nil) }

func (c *Conn) closeErr(err error) {
	if c.closed {
		return
	}
	c.closed = true
	syscall.EpollCtl(c.L.epfd, syscall.EPOLL_CTL_DEL, c.Fd, nil)
	syscall.Close(c.Fd)
	delete(c.L.conns, c.Fd)
	c.L.NConns.Add(-1)
	c.out = nil
	c.L.h.OnClose(c, err)
}

func (c *Conn) Closed() bool { return c.closed }
