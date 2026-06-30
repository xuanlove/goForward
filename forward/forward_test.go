package forward

import (
	"net"
	"testing"
	"time"
)

// fakeConn 实现 net.Conn 仅用于测试身份比较
type fakeConn struct{ id int }

func (f *fakeConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (f *fakeConn) Write(b []byte) (n int, err error)  { return 0, nil }
func (f *fakeConn) Close() error                       { return nil }
func (f *fakeConn) LocalAddr() net.Addr                { return nil }
func (f *fakeConn) RemoteAddr() net.Addr               { return nil }
func (f *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

func TestStopKey(t *testing.T) {
	tests := []struct {
		localPort string
		protocol  string
		want      string
	}{
		{"8080", "tcp", "8080tcp"},
		{"53", "udp", "53udp"},
		{"", "tcp", "tcp"},
	}
	for _, tt := range tests {
		got := StopKey(tt.localPort, tt.protocol)
		if got != tt.want {
			t.Errorf("StopKey(%q,%q)=%q want %q", tt.localPort, tt.protocol, got, tt.want)
		}
	}
}

func TestFormatTraffic(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{0, "0B"},
		{512, "512B"},
		{1024, "1.00KB"},
		{1048576, "1.00MB"},
		{1073741824, "1.00GB"},
		{2147483648, "2.00GB"},
	}
	for _, tt := range tests {
		got := formatTraffic(tt.bytes)
		if got != tt.want {
			t.Errorf("formatTraffic(%d)=%q want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestRemoveConn(t *testing.T) {
	a := &fakeConn{id: 1}
	b := &fakeConn{id: 2}
	c := &fakeConn{id: 3}
	conns := []net.Conn{a, b, c}
	conns = removeConn(conns, b)
	if len(conns) != 2 {
		t.Fatalf("移除后长度应为2，实际 %d", len(conns))
	}
	if conns[0] != a || conns[1] != c {
		t.Errorf("移除中间元素后剩余元素不正确")
	}
	// 移除不存在的元素
	conns = removeConn(conns, &fakeConn{id: 99})
	if len(conns) != 2 {
		t.Errorf("移除不存在元素不应改变长度")
	}
}
