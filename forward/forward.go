package forward

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"csz.net/goForward/conf"
	"csz.net/goForward/sql"
)

// ConnectionStats 转发运行态结构体
type ConnectionStats struct {
	conf.ConnectionStats
	TotalBytesOld  uint64       `gorm:"-"`
	TotalBytesLock sync.Mutex   `gorm:"-"`
	TCPConnections []net.Conn   `gorm:"-"` // 用于存储 TCP 连接
	connLock       sync.Mutex   `gorm:"-"` // 专用于保护 TCPConnections 切片
	TcpTime        int          `gorm:"-"` // TCP无传输时间
	activeConns    atomic.Int64 `gorm:"-"` // 当前活动 TCP 连接数（原子计数）
}

// 保存多个连接信息
type LargeConnectionStats struct {
	Connections []*ConnectionStats `json:"connections"`
}

// 复用缓冲区
var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

// StopKey 生成停止信号注册表使用的 key
func StopKey(localPort, protocol string) string {
	return localPort + protocol
}

// Run 开启转发，负责分发具体转发
func Run(stats *ConnectionStats) {
	defer releaseResources(stats)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := StopKey(stats.LocalPort, stats.Protocol)
	stopCh, unregister := conf.Registry.Register(key)
	defer unregister()

	var innerWg sync.WaitGroup

	// 流量统计与超时检测协程
	innerWg.Add(1)
	go func() {
		stats.printStats(ctx)
		innerWg.Done()
	}()

	slog.Info("开启转发", "protocol", stats.Protocol,
		"localPort", stats.LocalPort, "remoteAddr", stats.RemoteAddr, "remotePort", stats.RemotePort)

	// TCP 健康检查（可选）
	if stats.Protocol == "tcp" && stats.HealthCheck {
		innerWg.Add(1)
		go func() {
			stats.healthCheck(ctx)
			innerWg.Done()
		}()
	}

	switch stats.Protocol {
	case "udp":
		if err := stats.runUDP(ctx, cancel, stopCh); err != nil {
			slog.Error("UDP 监听失败", "port", stats.LocalPort, "err", err)
			sql.UpdateForwardStatus(stats.Id, conf.StatusAbnormal)
			return
		}
	default:
		stats.Protocol = "tcp"
		fallthrough
	case "tcp":
		if err := stats.runTCP(ctx, cancel, stopCh); err != nil {
			slog.Error("TCP 监听失败", "port", stats.LocalPort, "err", err)
			sql.UpdateForwardStatus(stats.Id, conf.StatusAbnormal)
			return
		}
	}

	innerWg.Wait()
}

// runTCP TCP 转发主循环
func (cs *ConnectionStats) runTCP(ctx context.Context, cancel context.CancelFunc, stopCh chan struct{}) error {
	listener, err := net.Listen("tcp", ":"+cs.LocalPort)
	if err != nil {
		return err
	}
	defer listener.Close()

	go func() {
		<-stopCh
		slog.Info("停止监听端口", "protocol", cs.Protocol, "port", cs.LocalPort)
		listener.Close()
		cancel()
		closeTCPConnections(cs)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		clientConn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("接受连接失败", "port", cs.LocalPort, "err", err)
			continue
		}
		// MaxConns 限制
		if cs.MaxConns > 0 && int(cs.activeConns.Load()) >= cs.MaxConns {
			slog.Warn("超过最大连接数限制，拒绝新连接", "port", cs.LocalPort, "max", cs.MaxConns)
			clientConn.Close()
			continue
		}
		go cs.handleTCPConnection(clientConn, ctx)
	}
}

// handleTCPConnection 处理单条 TCP 连接的双向转发
func (cs *ConnectionStats) handleTCPConnection(clientConn net.Conn, ctx context.Context) {
	defer clientConn.Close()
	remoteConn, err := net.Dial("tcp", net.JoinHostPort(cs.RemoteAddr, cs.RemotePort))
	if err != nil {
		slog.Warn("连接远程地址失败", "port", cs.LocalPort, "err", err)
		return
	}
	defer remoteConn.Close()

	// 登记连接
	cs.connLock.Lock()
	cs.TCPConnections = append(cs.TCPConnections, clientConn, remoteConn)
	cs.connLock.Unlock()
	cs.activeConns.Add(1)
	defer cs.activeConns.Add(-1)

	var copyWG sync.WaitGroup
	copyWG.Add(2)
	go func() {
		defer copyWG.Done()
		cs.copyBytes(clientConn, remoteConn)
	}()
	go func() {
		defer copyWG.Done()
		cs.copyBytes(remoteConn, clientConn)
	}()

	done := make(chan struct{})
	go func() {
		copyWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		clientConn.Close()
		remoteConn.Close()
		<-done
	}

	// 从切片中移除已关闭连接
	cs.connLock.Lock()
	cs.TCPConnections = removeConn(cs.TCPConnections, clientConn)
	cs.TCPConnections = removeConn(cs.TCPConnections, remoteConn)
	cs.connLock.Unlock()
}

// runUDP UDP 转发主循环（双向，兼容标准 UDP）
func (cs *ConnectionStats) runUDP(ctx context.Context, cancel context.CancelFunc, stopCh chan struct{}) error {
	localAddr, err := net.ResolveUDPAddr("udp", ":"+cs.LocalPort)
	if err != nil {
		return err
	}
	remoteAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(cs.RemoteAddr, cs.RemotePort))
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	go func() {
		<-stopCh
		slog.Info("停止监听端口", "protocol", cs.Protocol, "port", cs.LocalPort)
		conn.Close()
		cancel()
	}()

	// 远端响应转发回客户端的映射表：clientAddr.String() -> *net.UDPAddr
	var mapLock sync.Mutex
	clients := make(map[string]*net.UDPAddr)

	// 远端 -> 客户端 回包转发协程
	// 这里通过为每个客户端临时拨号到远端的方式实现：每收到一个客户端包，确保有一条到 remote 的 conn 用于读取响应。
	// 为简化实现，使用单条到 remote 的 UDP conn + 客户端映射 + 最近客户端指针。
	remoteConn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		slog.Warn("UDP 拨号远端失败", "port", cs.LocalPort, "err", err)
		return err
	}
	defer remoteConn.Close()

	// 读取远端响应并写回最近活跃的客户端（兼容简单请求-响应场景，如 DNS）
	go func() {
		buf := bufPool.Get().([]byte)
		defer bufPool.Put(buf)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := remoteConn.Read(buf)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("UDP 读取远端响应失败", "port", cs.LocalPort, "err", err)
				return
			}
			cs.TotalBytesLock.Lock()
			cs.TotalBytes += uint64(n)
			cs.TotalBytesLock.Unlock()
			// 广播回所有已知客户端
			mapLock.Lock()
			for _, c := range clients {
				conn.WriteToUDP(buf[:n], c)
			}
			mapLock.Unlock()
		}
	}()

	// 客户端 -> 远端
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		buf := bufPool.Get().([]byte)
		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			bufPool.Put(buf)
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("UDP 读取客户端失败", "port", cs.LocalPort, "err", err)
			return nil
		}
		cs.TotalBytesLock.Lock()
		cs.TotalBytes += uint64(n)
		cs.TotalBytesLock.Unlock()

		// 登记客户端地址
		mapLock.Lock()
		clients[clientAddr.String()] = clientAddr
		mapLock.Unlock()

		// 直接转发原始数据（不加长度头，兼容标准 UDP）
		go func(b []byte) {
			defer bufPool.Put(b)
			if _, err := remoteConn.Write(b); err != nil {
				slog.Warn("UDP 写入远端失败", "port", cs.LocalPort, "err", err)
			}
		}(buf[:n])
	}
}

// copyBytes 双向拷贝字节流
func (cs *ConnectionStats) copyBytes(dst, src net.Conn) error {
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			cs.TotalBytesLock.Lock()
			cs.TotalBytes += uint64(n)
			cs.TotalBytesLock.Unlock()
			if _, werr := dst.Write(buf[:n]); werr != nil {
				slog.Debug("写入目标失败", "port", cs.LocalPort, "err", werr)
				return werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Debug("读取源失败", "port", cs.LocalPort, "err", err)
			break
		}
	}
	dst.Close()
	src.Close()
	return nil
}

// printStats 定时统计流量并落库
func (cs *ConnectionStats) printStats(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cs.TotalBytesLock.Lock()
			// GB 进位
			var gb uint64 = 1073741824
			for cs.TotalBytes >= gb {
				cs.TotalGigabyte++
				cs.TotalBytes -= gb
			}
			// 不论是否有流量变化，都把当前值落库，避免进程异常丢失统计
			sql.UpdateForwardBytes(cs.Id, cs.TotalBytes)
			sql.UpdateForwardGb(cs.Id, cs.TotalGigabyte)

			if cs.TotalBytes > cs.TotalBytesOld {
				if cs.Protocol == "tcp" {
					cs.TcpTime = 0
				}
				total := formatTraffic(cs.TotalGigabyte*gb + cs.TotalBytes)
				slog.Info("流量统计", "protocol", cs.Protocol,
					"port", cs.LocalPort, "traffic", total,
					"conns", cs.activeConns.Load())
				cs.TotalBytesOld = cs.TotalBytes
			} else {
				if cs.Protocol == "tcp" {
					if cs.TcpTime >= conf.TcpTimeout {
						slog.Info("TCP 无传输超时，关闭所有连接", "port", cs.LocalPort)
						closeTCPConnections(cs)
						cs.TcpTime = 0
					} else {
						cs.TcpTime += 5
					}
				}
			}
			cs.TotalBytesLock.Unlock()
		case <-ctx.Done():
			// 退出前最后一次落库
			cs.TotalBytesLock.Lock()
			sql.UpdateForwardBytes(cs.Id, cs.TotalBytes)
			sql.UpdateForwardGb(cs.Id, cs.TotalGigabyte)
			cs.TotalBytesLock.Unlock()
			return
		}
	}
}

// healthCheck 周期性 TCP 远端探活
func (cs *ConnectionStats) healthCheck(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c, err := net.DialTimeout("tcp", net.JoinHostPort(cs.RemoteAddr, cs.RemotePort), 3*time.Second)
			if err != nil {
				slog.Warn("健康检查失败", "port", cs.LocalPort, "err", err)
			} else {
				c.Close()
			}
		case <-ctx.Done():
			return
		}
	}
}

// formatTraffic 把字节格式化为人类可读
func formatTraffic(bytes uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return strconv.FormatFloat(float64(bytes)/float64(GB), 'f', 2, 64) + "GB"
	case bytes >= MB:
		return strconv.FormatFloat(float64(bytes)/float64(MB), 'f', 2, 64) + "MB"
	case bytes >= KB:
		return strconv.FormatFloat(float64(bytes)/float64(KB), 'f', 2, 64) + "KB"
	default:
		return strconv.FormatUint(bytes, 10) + "B"
	}
}

// closeTCPConnections 关闭所有 TCP 连接并清空切片
func closeTCPConnections(stats *ConnectionStats) {
	stats.connLock.Lock()
	defer stats.connLock.Unlock()
	for i, conn := range stats.TCPConnections {
		if conn != nil {
			conn.Close()
		}
		stats.TCPConnections[i] = nil
	}
	stats.TCPConnections = stats.TCPConnections[:0]
}

// removeConn 从切片中移除指定连接
func removeConn(conns []net.Conn, target net.Conn) []net.Conn {
	for i, c := range conns {
		if c == target {
			// 移除并保留顺序（连接数通常不大，O(n) 可接受）
			return append(conns[:i], conns[i+1:]...)
		}
	}
	return conns
}

// releaseResources 释放资源
func releaseResources(stats *ConnectionStats) {
	closeTCPConnections(stats)
}
