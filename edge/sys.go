package edge

import (
	"fmt"
	"os"
	"strings"
)

func sscan(s string, a ...any) (int, error) { return fmt.Sscan(strings.TrimSpace(s), a...) }

// cpuSec is this process's user+system CPU time from /proc/self/stat.
func cpuSec() float64 {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	f := strings.Fields(s[i+2:])
	var ut, st float64
	fmt.Sscan(f[11], &ut)
	fmt.Sscan(f[12], &st)
	return (ut + st) / 100 // USER_HZ
}
