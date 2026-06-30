package sql

import (
	"testing"

	"csz.net/goForward/conf"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name      string
		input     conf.ConnectionStats
		wantPort  string
		wantProto string
		wantAddr  string
	}{
		{
			name:      "去空格+协议兜底",
			input:     conf.ConnectionStats{LocalPort: " 80 80 ", RemoteAddr: " 1.2.3.4 ", RemotePort: " 80 ", Protocol: "xxx"},
			wantPort:  "8080",
			wantAddr:  "1.2.3.4",
			wantProto: "tcp",
		},
		{
			name:      "udp 保留",
			input:     conf.ConnectionStats{LocalPort: "53", RemoteAddr: "8.8.8.8", RemotePort: "53", Protocol: "udp"},
			wantPort:  "53",
			wantAddr:  "8.8.8.8",
			wantProto: "udp",
		},
		{
			name:      "IPv6 字面量保留",
			input:     conf.ConnectionStats{LocalPort: "80", RemoteAddr: "2001:db8::1", RemotePort: "80", Protocol: "tcp"},
			wantPort:  "80",
			wantAddr:  "2001:db8::1",
			wantProto: "tcp",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.input
			Normalize(&f)
			if f.LocalPort != tt.wantPort {
				t.Errorf("LocalPort=%q want %q", f.LocalPort, tt.wantPort)
			}
			if f.RemoteAddr != tt.wantAddr {
				t.Errorf("RemoteAddr=%q want %q", f.RemoteAddr, tt.wantAddr)
			}
			if f.Protocol != tt.wantProto {
				t.Errorf("Protocol=%q want %q", f.Protocol, tt.wantProto)
			}
		})
	}
}
