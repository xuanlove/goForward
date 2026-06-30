package web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"csz.net/goForward/assets"
	"csz.net/goForward/conf"
	"csz.net/goForward/sql"
	"csz.net/goForward/utils"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
)

// 运行态引用，用于 /metrics 读取
var metricsProvider func() []ForwardMetric

// ForwardMetric /metrics 单条指标
type ForwardMetric struct {
	Id            int    `json:"id"`
	Protocol      string `json:"protocol"`
	LocalPort     string `json:"local_port"`
	RemoteAddr    string `json:"remote_addr"`
	RemotePort    string `json:"remote_port"`
	Status        int    `json:"status"`
	TotalBytes    uint64 `json:"total_bytes"`
	TotalGigabyte uint64 `json:"total_gigabyte"`
	ActiveConns   int64  `json:"active_conns"`
}

// SetMetricsProvider 注入 metrics 数据提供者
func SetMetricsProvider(p func() []ForwardMetric) {
	metricsProvider = p
}

func Run() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	store := cookie.NewStore(generateSessionSecret())
	store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   86400,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	r.Use(sessions.Sessions("goForward", store))
	r.Use(checkCookieMiddleware, csrfMiddleware)
	r.SetHTMLTemplate(template.Must(template.New("").Funcs(r.FuncMap).ParseFS(assets.Templates, "templates/*")))

	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{
			"forwardList": sql.GetForwardList(),
			"csrfToken":   getCSRFToken(c),
		})
	})
	r.GET("/ban", func(c *gin.Context) {
		c.JSON(200, sql.GetIpBan())
	})
	r.GET("/metrics", func(c *gin.Context) {
		if metricsProvider == nil {
			c.String(http.StatusOK, "")
			return
		}
		var sb strings.Builder
		sb.WriteString("# HELP goforward_total_bytes Total forwarded bytes per rule\n")
		sb.WriteString("# TYPE goforward_total_bytes counter\n")
		for _, m := range metricsProvider() {
			labels := fmt.Sprintf(`{id="%d",protocol="%s",local_port="%s"}`, m.Id, m.Protocol, m.LocalPort)
			fmt.Fprintf(&sb, "goforward_total_bytes%s %d\n", labels, m.TotalGigabyte*1073741824+m.TotalBytes)
			fmt.Fprintf(&sb, "goforward_active_conns%s %d\n", labels, m.ActiveConns)
		}
		c.Header("Content-Type", "text/plain; version=0.0.4")
		c.String(http.StatusOK, sb.String())
	})

	// 添加转发
	r.POST("/add", func(c *gin.Context) {
		f := parseForwardForm(c)
		if f.LocalPort == "" || f.RemoteAddr == "" || f.RemotePort == "" {
			msgPage(c, "添加失败，表单信息不完整", false)
			return
		}
		if utils.AddForward(f) {
			msgPage(c, "添加成功", true)
		} else {
			msgPage(c, "添加失败，端口已占用或与Web端口冲突", false)
		}
	})

	// 编辑转发
	r.POST("/edit/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			msgPage(c, "编辑失败，ID错误", false)
			return
		}
		f := parseForwardForm(c)
		f.Id = id
		if utils.EditForward(f) {
			msgPage(c, "编辑成功", true)
		} else {
			msgPage(c, "编辑失败", false)
		}
	})

	// 切换状态（POST + CSRF）
	r.POST("/do/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			msgPage(c, "操作失败，ID错误", false)
			return
		}
		f := sql.GetForward(id)
		if f.Id == 0 {
			msgPage(c, "操作失败，规则不存在", false)
			return
		}
		if f.Status == conf.StatusRunning {
			f.Status = conf.StatusStopped
			if len(sql.GetAction()) == 1 {
				msgPage(c, "停止失败，请确保有至少一个转发在运行", false)
				return
			}
		} else {
			f.Status = conf.StatusRunning
		}
		if utils.ExStatus(f) {
			msgPage(c, "操作成功", true)
		} else {
			msgPage(c, "操作失败", false)
		}
	})

	// 删除转发（POST + CSRF）
	r.POST("/del/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			msgPage(c, "删除失败，ID错误", false)
			return
		}
		f := sql.GetForward(id)
		if f.Id == 0 {
			msgPage(c, "删除失败，规则不存在", false)
			return
		}
		if len(sql.GetForwardList()) == 1 {
			msgPage(c, "删除失败，请确保有至少一个转发在运行", false)
			return
		}
		if utils.DelForward(f) {
			msgPage(c, "删除成功", true)
		} else {
			msgPage(c, "删除失败", false)
		}
	})

	// 导出
	r.GET("/export", func(c *gin.Context) {
		c.Header("Content-Disposition", `attachment; filename="goForward_export.json"`)
		c.JSON(http.StatusOK, sql.GetForwardList())
	})

	// 导入
	r.POST("/import", func(c *gin.Context) {
		file, err := c.FormFile("file")
		if err != nil {
			msgPage(c, "导入失败，未收到文件", false)
			return
		}
		f, err := file.Open()
		if err != nil {
			msgPage(c, "导入失败，文件打开失败", false)
			return
		}
		defer f.Close()
		var list []conf.ConnectionStats
		if err := json.NewDecoder(f).Decode(&list); err != nil {
			msgPage(c, "导入失败，JSON解析失败:"+err.Error(), false)
			return
		}
		s, sk := sql.ImportForwards(list)
		msgPage(c, fmt.Sprintf("导入完成，成功 %d 条，跳过 %d 条", s, sk), true)
	})

	r.GET("/pwd", func(c *gin.Context) {
		c.HTML(200, "pwd.tmpl", nil)
	})
	r.POST("/pwd", func(c *gin.Context) {
		if !sql.IpFree(c.ClientIP()) {
			msgPage(c, "IP is Ban", false)
			return
		}
		session := sessions.Default(c)
		session.Set("p", c.PostForm("p"))
		session.Options(sessions.Options{MaxAge: 86400})
		session.Save()
		if c.PostForm("p") != conf.WebPass {
			ban := conf.IpBan{
				Ip:        c.ClientIP(),
				TimeStamp: time.Now().Unix(),
			}
			sql.AddBan(ban)
		}
		c.Redirect(302, "/")
	})

	slog.Info("Web管理面板启动", "ip", conf.WebIP, "port", conf.WebPort)
	err := r.Run(net.JoinHostPort(conf.WebIP, conf.WebPort))
	if err != nil {
		slog.Error("Web启动失败", "err", err)
	}
}

// parseForwardForm 解析表单字段为 ConnectionStats
func parseForwardForm(c *gin.Context) conf.ConnectionStats {
	maxConns, _ := strconv.Atoi(c.PostForm("maxConns"))
	return conf.ConnectionStats{
		LocalPort:   c.PostForm("localPort"),
		RemotePort:  c.PostForm("remotePort"),
		RemoteAddr:  c.PostForm("remoteAddr"),
		Protocol:    c.PostForm("protocol"),
		Ps:          c.PostForm("ps"),
		MaxConns:    maxConns,
		HealthCheck: c.PostForm("healthCheck") == "on",
	}
}

// msgPage 渲染消息页
func msgPage(c *gin.Context, msg string, suc bool) {
	c.HTML(200, "msg.tmpl", gin.H{"msg": msg, "suc": suc})
}

// 密码验证中间件
func checkCookieMiddleware(c *gin.Context) {
	currenPath := c.Request.URL.Path
	if conf.WebPass != "" && currenPath != "/pwd" {
		session := sessions.Default(c)
		pass := session.Get("p")
		if pass != conf.WebPass {
			c.Redirect(http.StatusFound, "/pwd")
			c.Abort()
			return
		}
	}
	c.Next()
}

// csrfMiddleware 简易 CSRF 防护（double-submit cookie 模式）
// 对 POST/PUT/DELETE 请求校验表单 token 与 cookie token 一致
func csrfMiddleware(c *gin.Context) {
	if c.Request.Method != http.MethodPost {
		c.Next()
		return
	}
	// /pwd 不做 CSRF（首次登录无 token）
	if c.Request.URL.Path == "/pwd" {
		c.Next()
		return
	}
	session := sessions.Default(c)
	cookieToken, _ := session.Get("csrf").(string)
	if cookieToken == "" {
		// 首次访问生成 token
		cookieToken = randomToken(16)
		session.Set("csrf", cookieToken)
		session.Save()
	}
	formToken := c.PostForm("csrfToken")
	if formToken == "" || formToken != cookieToken {
		c.String(http.StatusForbidden, "CSRF校验失败")
		c.Abort()
		return
	}
	c.Next()
}

// getCSRFToken 从 session 获取 CSRF token
func getCSRFToken(c *gin.Context) string {
	session := sessions.Default(c)
	t, _ := session.Get("csrf").(string)
	if t == "" {
		t = randomToken(16)
		session.Set("csrf", t)
		session.Save()
	}
	return t
}

// generateSessionSecret 生成随机 session 密钥
func generateSessionSecret() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// 极端情况退化为固定值（不应发生）
		return []byte("goForward-fallback-secret-please")
	}
	return b
}

// randomToken 生成 hex 随机 token
func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}
