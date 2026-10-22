package bench

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// RESP is one connection to a Redis-protocol server (Redis or Valkey). It is
// the few lines the comparison path needs, so the module takes no client
// library for it. A RESP is not safe for concurrent use: each worker dials
// its own.
type RESP struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

// DialRESP connects to addr (host:port).
func DialRESP(addr string) (*RESP, error) {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &RESP{c: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}, nil
}

func (c *RESP) Close() error { return c.c.Close() }

// Do sends one command and reads one reply. An integer reply is returned in
// n, a simple or bulk string in s. A nested array reply is read and dropped.
func (c *RESP) Do(args ...string) (n int64, s string, err error) {
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
	if err := c.w.Flush(); err != nil {
		return 0, "", err
	}
	return c.read()
}

func (c *RESP) line() (string, error) {
	l, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(l) < 3 {
		return "", errors.New("resp: short line")
	}
	return l[:len(l)-2], nil
}

func (c *RESP) read() (int64, string, error) {
	l, err := c.line()
	if err != nil {
		return 0, "", err
	}
	switch l[0] {
	case ':':
		n, err := strconv.ParseInt(l[1:], 10, 64)
		return n, "", err
	case '+':
		return 0, l[1:], nil
	case '-':
		return 0, "", errors.New("resp: " + l[1:])
	case '$':
		size, err := strconv.Atoi(l[1:])
		if err != nil || size < 0 {
			return 0, "", err
		}
		buf := make([]byte, size+2)
		if _, err := ioReadFull(c.r, buf); err != nil {
			return 0, "", err
		}
		return 0, string(buf[:size]), nil
	case '*':
		count, err := strconv.Atoi(l[1:])
		if err != nil {
			return 0, "", err
		}
		for i := 0; i < count; i++ {
			if _, _, err := c.read(); err != nil {
				return 0, "", err
			}
		}
		return int64(count), "", nil
	}
	return 0, "", fmt.Errorf("resp: unexpected reply %q", l)
}

func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
