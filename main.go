package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"csz.net/goForward/conf"
	"csz.net/goForward/forward"
	"csz.net/goForward/sql"
	"csz.net/goForward/web"

	"github.com/pelletier/go-toml/v2"
)

func main() {
	// 初始化停止信号注册表
	conf.Registry = conf.NewStopRegistry()

	// 启动 Web 服务（返回 *http.Server 用于优雅关闭）
	httpServer := web.Run()
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
			TotalBytesOld: f.TotalBytes,
		}
		conf.Wg.Add(1)
		go func(s *forward.ConnectionStats) {
			forward.Run(s)
			conf.Wg.Done()
		}(stats)
	}

	// 监听退出信号，优雅关闭：先停所有转发，再停 Web
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("收到退出信号，开始优雅关闭", "signal", sig)
		conf.Registry.StopAll()
	}()

	conf.Wg.Wait()
	// 所有转发协程已退出，现在优雅关闭 Web 服务
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			slog.Warn("Web 服务关闭异常", "err", err)
		}
	}
	slog.Info("所有转发已停止，进程退出")
}

// collectMetrics 收集所有运行态实例的指标（通过 forward 包统一注册表）
func collectMetrics() []web.ForwardMetric {
	all := forward.AllRuntime()
	list := make([]web.ForwardMetric, 0, len(all))
	for _, s := range all {
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
			ActiveConns:   s.ActiveConns(),
		}
		s.TotalBytesLock.Unlock()
		list = append(list, m)
	}
	return list
}

func init() {
	// slog 必须在所有其他初始化（配置加载、数据库）之前设置，保证日志格式统一
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

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
