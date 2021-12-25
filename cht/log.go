package cht

import (
	"bufio"
	"io"
	"strings"
)

type logInfo struct {
	Addr  string
	Ready bool
}

// logProxy returns io.Writer that can be used as mongo log output.
//
// The io.Writer will parse json logs and write them to provided logger.
// Call context.CancelFunc on mongo exit.
func logProxy(f func(info logInfo)) io.Writer {
	r, w := io.Pipe()

	s := bufio.NewScanner(r)

	go func() {
		for s.Scan() {
			t := strings.TrimSpace(s.Text())
			if strings.Contains(t, "Application: Ready for connections") {
				f(logInfo{Ready: true})
			}
			if !strings.Contains(s.Text(), "Application: Listening for") {
				continue
			}

			elems := strings.Split(t, " ")
			f(logInfo{Addr: elems[len(elems)-1]})
		}
	}()

	return w
}
