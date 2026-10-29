package evloop

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

type echo struct {
	mu     sync.Mutex
	got    map[*Conn][]byte
	opened chan *Conn
	server bool
}

func (e *echo) OnOpen(c *Conn) {
	if e.opened != nil {
		e.opened <- c
	}
}
func (e *echo) OnData(c *Conn, p []byte) {
	if e.server {
		c.Write(p)
		return
	}
	e.mu.Lock()
	e.got[c] = append(e.got[c], p...)
	e.mu.Unlock()
}
func (e *echo) OnWritable(c *Conn)       {}
func (e *echo) OnClose(c *Conn, _ error) {}

func TestEchoManyConnsAndLargeWrites(t *testing.T) {
	srv, err := New(0, &echo{server: true}, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen("127.0.0.1:0", 1024); err != nil {
		t.Skip("listen:", err)
	}
	// find the bound port
	sa, _ := getsockname(srv.lfd)
	go srv.Run()
	cl := &echo{got: map[*Conn][]byte{}, opened: make(chan *Conn, 100)}
	cli, _ := New(1, cl, 64<<10)
	go cli.Run()
	const n = 50
	payload := bytes.Repeat([]byte("roomcast"), 64<<10) // 512 KiB: forces EAGAIN and EPOLLOUT
	for i := 0; i < n; i++ {
		cli.Submit(func() {
			if _, err := cli.Dial(sa, "127.0.0.1", nil); err != nil {
				t.Error(err)
			}
		})
	}
	var conns []*Conn
	for i := 0; i < n; i++ {
		select {
		case c := <-cl.opened:
			conns = append(conns, c)
		case <-time.After(5 * time.Second):
			t.Fatal("connect timeout")
		}
	}
	for _, c := range conns[:5] {
		c := c
		cli.Submit(func() { c.Write(payload) })
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cl.mu.Lock()
		done := 0
		for _, c := range conns[:5] {
			if len(cl.got[c]) == len(payload) {
				done++
			}
		}
		cl.mu.Unlock()
		if done == 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for _, c := range conns[:5] {
		if !bytes.Equal(cl.got[c], payload) {
			t.Fatalf("echo mismatch: %d of %d bytes", len(cl.got[c]), len(payload))
		}
	}
	if srv.NConns.Load() != n {
		t.Fatalf("server conns %d", srv.NConns.Load())
	}
}
