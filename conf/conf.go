package conf

import (
	"fmt"
	"sync"
)

// 状态常量
const (
	StatusRunning  = 0
	StatusStopped  = 1
	StatusAbnormal = 2 // 监听失败或远端不可达
)

// ConnectionStats 结构体用于保存多个连接信息
type ConnectionStats struct {
	Id            int `gorm:"primaryKey;autoIncrement"`
	Ps            string
	LocalPort     string
	RemoteAddr    string
	RemotePort    string
	Protocol      string
	Status        int
	TotalBytes    uint64
	TotalGigabyte uint64
	MaxConns      int  // 单条规则最大并发 TCP 连接，0 表示不限制
	HealthCheck   bool // 是否启用远端 TCP 探活（仅 TCP）
}

type IpBan struct {
	Id        int `gorm:"primaryKey;autoIncrement"`
	Ip        string
	TimeStamp int64
}

// 全局转发协程等待组
var Wg sync.WaitGroup

// Web管理面板端口
var WebPort string

// Web IP绑定
var WebIP string

// Web管理面板密码
var WebPass string

// TCP超时
var TcpTimeout int

// 版本号
var version string

// 数据库位置
var Db string

// 配置文件路径（可选）
var ConfigFile string

// Version 返回当前版本号
func Version() string {
	return version
}

// StopRegistry 转发停止信号注册表
// 替代原先的全局广播通道 conf.Ch，按 key=localPort+protocol 精确投递停止信号。
type StopRegistry struct {
	mu    sync.Mutex
	chans map[string]chan struct{}
}

// NewStopRegistry 创建注册表
func NewStopRegistry() *StopRegistry {
	return &StopRegistry{chans: make(map[string]chan struct{})}
}

// Register 注册一条转发规则对应的停止通道，返回该通道与反注册函数
func (r *StopRegistry) Register(key string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	r.chans[key] = ch
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		if cur, ok := r.chans[key]; ok && cur == ch {
			delete(r.chans, key)
		}
		r.mu.Unlock()
	}
}

// Lookup 获取指定 key 的停止通道（不存在返回 nil）
func (r *StopRegistry) Lookup(key string) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.chans[key]
}

// Stop 向指定 key 投递停止信号，返回是否命中
func (r *StopRegistry) Stop(key string) bool {
	ch := r.Lookup(key)
	if ch == nil {
		return false
	}
	select {
	case ch <- struct{}{}:
	default:
	}
	return true
}

// StopAll 向所有已注册转发投递停止信号
func (r *StopRegistry) StopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ch := range r.chans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Registry 全局停止信号注册表，由 main 初始化
var Registry *StopRegistry

// FileConfig 可选的配置文件结构（TOML）
type FileConfig struct {
	WebPort    string `toml:"web_port"`
	WebIP      string `toml:"web_ip"`
	WebPass    string `toml:"web_pass"`
	Db         string `toml:"db"`
	TcpTimeout int    `toml:"tcp_timeout"`
}

func init() {
	if version != "" {
		fmt.Println("goForward Version " + version)
	}
}
