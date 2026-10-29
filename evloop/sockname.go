package evloop

import (
	"fmt"
	"syscall"
)

func getsockname(fd int) (string, error) {
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		return "", err
	}
	if a, ok := sa.(*syscall.SockaddrInet4); ok {
		return fmt.Sprintf("%d.%d.%d.%d:%d", a.Addr[0], a.Addr[1], a.Addr[2], a.Addr[3], a.Port), nil
	}
	return "", fmt.Errorf("not inet4")
}

// Addr is the listening address of a loop that called Listen.
func (l *Loop) Addr() (string, error) { return getsockname(l.lfd) }

// SO_REUSEPORT on Linux (not exported by package syscall).
const soReusePort = 0xf
