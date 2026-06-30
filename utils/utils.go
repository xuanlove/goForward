package utils

import (
	"sync"

	"csz.net/goForward/conf"
	"csz.net/goForward/forward"
	"csz.net/goForward/sql"
)

// 增加转发并开启
func AddForward(newF conf.ConnectionStats) bool {
	if newF.LocalPort == conf.WebPort && newF.Protocol == "tcp" {
		return false
	}
	id := sql.AddForward(newF)
	if id > 0 {
		startForward(id, newF)
		return true
	}
	return false
}

// startForward 起一个转发协程并登记到 WaitGroup
func startForward(id int, f conf.ConnectionStats) {
	stats := &forward.ConnectionStats{
		ConnectionStats: conf.ConnectionStats{
			Id:          id,
			LocalPort:   f.LocalPort,
			RemotePort:  f.RemotePort,
			RemoteAddr:  f.RemoteAddr,
			Protocol:    f.Protocol,
			TotalBytes:  f.TotalBytes,
			MaxConns:    f.MaxConns,
			HealthCheck: f.HealthCheck,
		},
		TotalBytesOld:  f.TotalBytes,
		TotalBytesLock: sync.Mutex{},
	}
	conf.Wg.Add(1)
	go func() {
		forward.Run(stats)
		conf.Wg.Done()
	}()
}

// 删除并关闭指定转发
func DelForward(f conf.ConnectionStats) bool {
	sql.DelForward(f.Id)
	conf.Registry.Stop(forward.StopKey(f.LocalPort, f.Protocol))
	return true
}

// EditForward 编辑转发规则：停止旧实例（端口/协议可能变更），更新库，再启动新实例
func EditForward(f conf.ConnectionStats) bool {
	old := sql.GetForward(f.Id)
	if old.Id == 0 {
		return false
	}
	// 停止旧实例以应用新配置（端口/协议变化时按旧 key 停止）
	conf.Registry.Stop(forward.StopKey(old.LocalPort, old.Protocol))
	f.Status = conf.StatusRunning
	if !sql.UpdateForward(f) {
		return false
	}
	startForward(f.Id, f)
	return true
}

// 改变转发状态
func ExStatus(f conf.ConnectionStats) bool {
	if sql.FreeForward(f.LocalPort, f.Protocol) {
		return false
	}
	if sql.UpdateForwardStatus(f.Id, f.Status) {
		// 启用转发
		if f.Status == conf.StatusRunning {
			startForward(f.Id, f)
			return true
		} else {
			conf.Registry.Stop(forward.StopKey(f.LocalPort, f.Protocol))
			return true
		}
	}
	return false
}
