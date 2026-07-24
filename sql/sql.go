package sql

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"csz.net/goForward/conf"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// 定义数据库指针
var db *gorm.DB

// Once 初始化数据库
func Once() {
	var err error
	var dbPath string
	executablePath, err := os.Executable()
	if conf.Db == "goForward.db" {
		if err != nil {
			slog.Warn("获取可执行文件路径失败，使用默认路径", "err", err)
			dbPath = "goForward.db"
		} else {
			dbPath = filepath.Join(filepath.Dir(executablePath), "goForward.db")
		}
	} else {
		dbPath = conf.Db
	}
	slog.Info("数据库路径", "path", dbPath)
	db, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		slog.Error("连接数据库失败", "err", err)
		return
	}
	db.AutoMigrate(&conf.ConnectionStats{})
	db.AutoMigrate(&conf.IpBan{})
}

// 获取转发列表
func GetForwardList() []conf.ConnectionStats {
	var res []conf.ConnectionStats
	db.Model(&conf.ConnectionStats{}).Find(&res)
	return res
}

// 获取启用的转发列表
func GetAction() []conf.ConnectionStats {
	var res []conf.ConnectionStats
	db.Model(&conf.ConnectionStats{}).Where("status = ?", conf.StatusRunning).Find(&res)
	return res
}

// 获取ipban列表
func GetIpBan() []conf.IpBan {
	var res []conf.IpBan
	db.Model(&conf.IpBan{}).Find(&res)
	return res
}

// 修改指定转发统计流量(byte)
func UpdateForwardBytes(id int, bytes uint64) bool {
	res := db.Model(&conf.ConnectionStats{}).Where("id = ?", id).Update("total_bytes", bytes)
	if res.Error != nil {
		slog.Warn("更新流量失败", "id", id, "err", res.Error)
		return false
	}
	return true
}

// 修改指定转发统计流量(GB)
func UpdateForwardGb(id int, gb uint64) bool {
	res := db.Model(&conf.ConnectionStats{}).Where("id = ?", id).Update("total_gigabyte", gb)
	if res.Error != nil {
		slog.Warn("更新GB失败", "id", id, "err", res.Error)
		return false
	}
	return true
}

// 修改指定转发状态
func UpdateForwardStatus(id int, status int) bool {
	res := db.Model(&conf.ConnectionStats{}).Where("id = ?", id).Update("status", status)
	if res.Error != nil {
		slog.Warn("更新状态失败", "id", id, "err", res.Error)
		return false
	}
	return true
}

// 获取指定转发内容
func GetForward(id int) conf.ConnectionStats {
	var get conf.ConnectionStats
	db.Model(&conf.ConnectionStats{}).Where("id = ?", id).Find(&get)
	return get
}

// 判断指定端口转发是否可添加
func FreeForward(localPort, protocol string) bool {
	var get conf.ConnectionStats
	res := db.Model(&conf.ConnectionStats{}).Where("local_port = ? And protocol = ?", localPort, protocol).Find(&get)
	if res.Error == nil {
		if get.Id == 0 {
			return true
		} else {
			return false
		}
	}
	return false
}

// 去掉所有空格
func rmSpaces(input string) string {
	return strings.ReplaceAll(input, " ", "")
}

// Normalize 规范化转发数据（去空格、协议兜底）
func Normalize(newForward *conf.ConnectionStats) {
	newForward.RemoteAddr = rmSpaces(newForward.RemoteAddr)
	newForward.RemotePort = rmSpaces(newForward.RemotePort)
	newForward.LocalPort = rmSpaces(newForward.LocalPort)
	if newForward.Protocol != "udp" {
		newForward.Protocol = "tcp"
	}
}

// 增加转发
func AddForward(newForward conf.ConnectionStats) int {
	Normalize(&newForward)
	if !FreeForward(newForward.LocalPort, newForward.Protocol) {
		return 0
	}
	tx := db.Begin()
	if tx.Error != nil {
		slog.Warn("开启事务失败", "err", tx.Error)
		return 0
	}
	if err := tx.Create(&newForward).Error; err != nil {
		slog.Warn("插入新转发失败", "err", err)
		tx.Rollback()
		return 0
	}
	tx.Commit()
	return newForward.Id
}

// UpdateForward 编辑已有转发规则（仅修改字段，不影响运行态统计）
func UpdateForward(f conf.ConnectionStats) bool {
	Normalize(&f)
	tx := db.Begin()
	if tx.Error != nil {
		return false
	}
	if err := tx.Model(&conf.ConnectionStats{}).Where("id = ?", f.Id).
		Updates(map[string]interface{}{
			"local_port":   f.LocalPort,
			"remote_addr":  f.RemoteAddr,
			"remote_port":  f.RemotePort,
			"protocol":     f.Protocol,
			"ps":           f.Ps,
			"max_conns":    f.MaxConns,
			"health_check": f.HealthCheck,
		}).Error; err != nil {
		slog.Warn("更新转发失败", "err", err)
		tx.Rollback()
		return false
	}
	tx.Commit()
	return true
}

// 删除转发
func DelForward(id int) bool {
	if err := db.Where("id = ?", id).Delete(&conf.ConnectionStats{}).Error; err != nil {
		slog.Warn("删除转发失败", "err", err)
		return false
	}
	return true
}

// ImportForwards 批量导入转发规则，返回成功导入的规则列表（含分配的 Id）与跳过条数
// 已存在的（按 local_port+protocol）或必填字段为空的会被跳过。
func ImportForwards(list []conf.ConnectionStats) (added []conf.ConnectionStats, skipped int) {
	for i := range list {
		f := list[i]
		Normalize(&f)
		// 验证必填字段，避免导入空规则导致监听失败
		if f.LocalPort == "" || f.RemoteAddr == "" || f.RemotePort == "" {
			skipped++
			continue
		}
		if !FreeForward(f.LocalPort, f.Protocol) {
			skipped++
			continue
		}
		if id := AddForward(f); id > 0 {
			f.Id = id
			f.Status = conf.StatusRunning
			added = append(added, f)
		} else {
			skipped++
		}
	}
	return
}

// 增加错误登录
func AddBan(ip conf.IpBan) bool {
	tx := db.Begin()
	if tx.Error != nil {
		return false
	}
	if err := tx.Create(&ip).Error; err != nil {
		slog.Warn("记录封禁失败", "err", err)
		tx.Rollback()
		return false
	}
	tx.Commit()
	return true
}

// 检查过去一天内指定IP地址的记录条数是否超过三条
func IpFree(ip string) bool {
	oneDayAgo := time.Now().Add(-24 * time.Hour).Unix()
	var count int64
	if err := db.Model(&conf.IpBan{}).Where("ip = ? AND time_stamp > ?", ip, oneDayAgo).Count(&count).Error; err != nil {
		slog.Warn("查询封禁计数失败", "err", err)
		return false
	}
	return count < 3
}
