使用 golang 实现的 tcp udp 端口转发

目前已实现：

- TCP / UDP 端口转发（UDP 每客户端独立会话，避免多客户端串包）
- 规则热加载（增删改无需重启，即时生效）
- Web 管理面板（规则管理、状态切换、流量统计）
- 流量统计（按规则统计字节流量并持久化）
- 规则导入导出（JSON 格式，支持批量迁移）
- Prometheus 指标（`/metrics` 暴露流量与活动连接数）
- TCP 最大连接数限制（按规则限流）
- TCP 远端健康检查（每 30 秒探活）
- TCP 无传输超时关闭（防止空连接占用）
- Web 面板密码保护（24 小时内同 IP 试错超 3 次自动封禁）
- CSRF 防护（double-submit cookie 模式）
- 优雅关闭（收到退出信号后先停转发再停 Web）
- 可选 TOML 配置文件（命令行参数优先级更高）

支持：Linux、Windows、MacOS（MacOS 需要自行编译）

**截图**

![image](https://github.com/csznet/goForward/assets/127601663/2f7840ff-9b34-4f69-a7c1-41feb35e726b)

**使用**

Linux 下载

```
sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/xuanlove/goForward/main/get.sh)"
```

运行

```
./goForward
```

**参数**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-port` | Web 管理面板端口 | 8889 |
| `-ip` | Web 管理面板绑定 IP | 0.0.0.0 |
| `-pass` | Web 管理面板访问密码 | 空（不启用密码） |
| `-db` | 数据库文件路径 | goForward.db |
| `-tt` | TCP 无传输超时关闭时间（秒，最小 5） | 60 |
| `-config` | TOML 配置文件路径（可选） | 空 |

示例：

```
./goForward -port 8899 -pass 666 -tt 18000
```

**配置文件**

通过 `-config` 指定 TOML 配置文件，命令行显式设置的参数优先级更高：

```toml
web_port = "8899"
web_ip = "0.0.0.0"
web_pass = "666"
db = "/root/data.db"
tcp_timeout = 18000
```

**密码保护**

设置 `-pass` 后访问 Web 面板需要输入密码。当 24 小时内同一 IP 密码试错超过 3 次将会被封禁。

**规则导入导出**

在 Web 面板右上角可导出当前所有规则为 JSON 文件，或导入 JSON 文件批量添加规则。已存在的端口与协议组合会被跳过。

**Prometheus 指标**

访问 `/metrics` 获取 Prometheus 格式指标：

- `goforward_total_bytes`：每条规则的累计转发流量
- `goforward_active_conns`：每条规则的当前活动 TCP 连接数

## 开机自启

**创建 Systemd 服务**

```
sudo nano /etc/systemd/system/goForward.service
```

**输入内容**

```
[Unit]
Description=Start goForward on boot

[Service]
ExecStart=/full/path/to/your/goForward -pass 666

[Install]
WantedBy=default.target
```

其中的`/full/path/to/your/goForward`改为二进制文件地址，后面可接参数

**重新加载 Systemd 配置**

```
sudo systemctl daemon-reload
```

**启用服务**

```
sudo systemctl enable goForward
```

**启动服务**

```
sudo systemctl start goForward
```

**检查状态**

```
sudo systemctl status goForward.service
```
