package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"csz.net/goForward/conf"
	"csz.net/goForward/forward"
	"csz.net/goForward/sql"
	"csz.net/goForward/web"

	"github.com/pelletier/go-toml/v2"
)

// 运行态转发实例（用于 metrics 读取）
var runtimeForwards = struct {
	sync.RWMutex
	m map[int]*forward.ConnectionStats
}{m: make(map[int]*forward.ConnectionStats)}

func main() {
	// slog 默认 JSON 输出到 stdout
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// 初始化停止信号注册表
	conf.Registry = conf.NewStopRegistry()

	go web.Run()
	web.SetMetricsProvider(collectMetrics)

	if conf.TcpTimeout < 5 {
		conf.TcpTimeout = 5
	}

	forwardList := sql.GetAction()
	if len(forwardList) == 0 {
		// 添加测试数据
		testData := conf.ConnectionStats{
			LocalPort:  conf.WebPort,
			RemotePort: conf.WebPort,
			RemoteAddr: "127.0.0.1",
			Protocol:   "udp",
		}
		sql.AddForward(testData)
		forwardList = sql.GetForwardList()
	}

	for i := range forwardList {
		f := forwardList[i]
		stats := &forward.ConnectionStats{
			ConnectionStats: conf.ConnectionStats{
				Id:            f.Id,
				Protocol:      f.Protocol,
				LocalPort:     f.LocalPort,
				RemotePort:    f.RemotePort,
				RemoteAddr:    f.RemoteAddr,
				TotalBytes:    f.TotalBytes,
				TotalGigabyte: f.TotalGigabyte,
				MaxConns:      f.MaxConns,
				HealthCheck:   f.HealthCheck,
			},
			TotalBytesOld:  f.TotalBytes,
			TotalBytesLock: sync.Mutex{},
		}
		registerRuntime(stats)
		conf.Wg.Add(1)
		go func(s *forward.ConnectionStats) {
			forward.Run(s)
			conf.Wg.Done()
		}(stats)
	}

	// 监听退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("收到退出信号，开始优雅关闭", "signal", sig)
		conf.Registry.StopAll()
	}()

	conf.Wg.Wait()
	slog.Info("所有转发已停止，进程退出")
}

// registerRuntime 登记运行态实例
func registerRuntime(s *forward.ConnectionStats) {
	runtimeForwards.Lock()
	runtimeForwards.m[s.Id] = s
	runtimeForwards.Unlock()
}

// collectMetrics 收集所有运行态实例的指标
func collectMetrics() []web.ForwardMetric {
	runtimeForwards.RLock()
	defer runtimeForwards.RUnlock()
	list := make([]web.ForwardMetric, 0, len(runtimeForwards.m))
	for _, s := range runtimeForwards.m {
		s.TotalBytesLock.Lock()
		m := web.ForwardMetric{
			Id:            s.Id,
			Protocol:      s.Protocol,
			LocalPort:     s.LocalPort,
			RemoteAddr:    s.RemoteAddr,
			RemotePort:    s.RemotePort,
			Status:        s.Status,
			TotalBytes:    s.TotalBytes,
			TotalGigabyte: s.TotalGigabyte,
			ActiveConns:   0, // 由 forward 内部 atomic 提供；这里简化，实际可通过方法暴露
		}
		s.TotalBytesLock.Unlock()
		list = append(list, m)
	}
	return list
}

func init() {
	flag.StringVar(&conf.WebPort, "port", "8889", "Web Port")
	flag.StringVar(&conf.Db, "db", "goForward.db", "Db Path")
	flag.StringVar(&conf.WebIP, "ip", "0.0.0.0", "Web IP")
	flag.StringVar(&conf.WebPass, "pass", "", "Web Password")
	flag.IntVar(&conf.TcpTimeout, "tt", 60, "Tcp Timeout")
	flag.StringVar(&conf.ConfigFile, "config", "", "Optional TOML config file path")
	flag.Parse()

	// 加载配置文件（若指定），命令行参数优先级更高：仅当命令行未显式设置时才用配置文件覆盖
	if conf.ConfigFile != "" {
		setFlags := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
		loadConfigFile(conf.ConfigFile, setFlags)
	}

	if !strings.HasSuffix(conf.Db, ".db") {
		conf.Db += ".db"
	}
	sql.Once()
}

// loadConfigFile 从 TOML 文件加载配置
// setFlags 记录命令行显式设置过的 flag，仅未显式设置的项才被配置文件覆盖。
func loadConfigFile(path string, setFlags map[string]bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("读取配置文件失败", "path", path, "err", err)
		return
	}
	var fc conf.FileConfig
	if err := toml.Unmarshal(b, &fc); err != nil {
		slog.Warn("解析配置文件失败", "err", err)
		return
	}
	if fc.WebPort != "" && !setFlags["port"] {
		conf.WebPort = fc.WebPort
	}
	if fc.WebIP != "" && !setFlags["ip"] {
		conf.WebIP = fc.WebIP
	}
	if fc.WebPass != "" && !setFlags["pass"] {
		conf.WebPass = fc.WebPass
	}
	if fc.Db != "" && !setFlags["db"] {
		conf.Db = fc.Db
	}
	if fc.TcpTimeout > 0 && !setFlags["tt"] {
		conf.TcpTimeout = fc.TcpTimeout
	}
	slog.Info("已加载配置文件", "path", path)
}
