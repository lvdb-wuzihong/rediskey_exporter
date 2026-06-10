# Redis Key Analyzer

Redis 诊断守护进程，常驻采集**慢日志**、**大Key**、**热Key**，输出结构化 JSON 日志文件，适配云 Redis 环境。

---

## 项目结构

```
rediskey_exporter/
├── config.yaml      # 配置文件（Redis连接、采集参数、输出路径）
├── config.py        # 配置加载模块
├── main.py          # 守护进程入口
├── slowlog.py       # 慢日志采集模块
├── bigkey.py        # 大Key 扫描模块
├── hotkey.py        # 热Key 采集模块
└── requirements.txt # Python 依赖
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 编辑配置文件

```bash
vim config.yaml
```

### 3. 启动

```bash
# 默认读取当前目录 config.yaml
python3 main.py

# 指定配置文件
python3 main.py --config /opt/rediskey_exporter/config.yaml
```

### 4. 后台运行

```bash
# 使用 nohup
nohup python3 main.py --config /opt/rediskey_exporter/config.yaml > /dev/null 2>&1 &

# 使用 systemd（推荐）
# 创建 /etc/systemd/system/redis-analyzer.service 后：
systemctl start redis-analyzer
systemctl enable redis-analyzer
```

### 5. 停止

```bash
# Ctrl+C 或 kill 信号优雅退出
kill <pid>
```

---

## 配置文件 `config.yaml`

```yaml
# Redis 连接
redis:
  host: "127.0.0.1"
  port: 6379
  password: null
  db: 0
  socket_timeout: 10

# 慢日志采集
slowlog:
  enabled: true
  interval: 60              # 采集间隔（秒）
  count: 128                # 每次拉取条数
  min_duration_ms: 100      # 自定义过滤阈值（毫秒），低于此值不记录
  reset_after_collect: false # 采集后是否重置慢日志

# 大Key扫描
bigkey:
  enabled: true
  interval: 600             # 扫描间隔（秒），开销较大建议设长
  threshold_bytes: 10485760 # 阈值（字节），默认10MB
  scan_count: 1000          # 每次SCAN的count
  max_keys: 0               # 最多扫描key数，0=不限制

# 热Key探测
hotkey:
  enabled: true
  interval: 120             # 采集间隔（秒）
  method: "auto"            # auto / monitor / freq
  sample_seconds: 5         # MONITOR采样时长（秒）
  top_n: 20                 # 返回前N个热Key

# 日志输出
output:
  log_dir: "./logs"
  slowlog_file: "slowlog.json"
  bigkey_file: "bigkey.json"
  hotkey_file: "hotkey.json"
```

### 关键参数说明

| 参数 | 说明 |
|------|------|
| `slowlog.min_duration_ms` | 自定义慢日志过滤阈值（毫秒）。云Redis无法修改`slowlog-log-slower-than`时，用此参数在客户端过滤 |
| `slowlog.interval` | 慢日志采集间隔，建议 30-120 秒 |
| `bigkey.interval` | 大Key扫描间隔，全量SCAN开销大，建议 300-3600 秒 |
| `hotkey.method` | `auto`=自动选择（优先OBJECT FREQ，降级MONITOR），`monitor`=MONITOR采样，`freq`=OBJECT FREQ（需LFU策略） |
| `hotkey.sample_seconds` | MONITOR采样时长，建议3-10秒，不宜过长 |

---

## 输出格式

日志文件为**行式 JSON**（每行一条记录），追加写入：

**logs/slowlog.json**
```json
{"time": "2026-06-10T02:54:52.489Z", "type": "slowlog", "addr": "10.81.129.4:6379", "id": 1, "cmd": "KEYS *", "duration_us": 125000, "duration_ms": 125.0, "start_time": "2026-06-10T02:50:00Z", "client_address": "10.0.0.2:54321", "client_name": ""}
```

**logs/bigkey.json**
```json
{"time": "2026-06-10T02:55:00.000Z", "type": "bigkey", "addr": "10.81.129.4:6379", "key": "user:sessions:data", "data_type": "hash", "memory_bytes": 52428800, "memory_mb": 50.0, "elements": 150000, "ttl": -1}
```

**logs/hotkey.json**
```json
{"time": "2026-06-10T02:55:05.000Z", "type": "hotkey", "addr": "10.81.129.4:6379", "key": "product:detail:10086", "access_count": 5823, "sample_seconds": 5, "total_ops": 12000, "access_ratio": 0.4853, "data_type": "string", "ttl": 3600}
```

---

## 对接 Grafana

### 方案一：Loki（推荐）

1. 部署 Loki + Promtail
2. Promtail 采集 `./logs/*.json` 日志文件
3. Grafana 添加 Loki 数据源，查询慢日志/大Key/热Key

### 方案二：直接查文件

使用 Grafana 的 Infinity 插件或 JSON 数据源直接读取日志文件。

---

## 云 Redis 兼容性

- `CONFIG` 命令被禁用时自动降级（慢日志配置显示为 unknown）
- 慢日志 `min_duration_ms` 在客户端过滤，无需修改 Redis 配置
- `MONITOR` 命令在部分云环境可能受限，如遇报错可改用 `freq` 方式或禁用热Key探测

---

## systemd 示例

创建 `/etc/systemd/system/redis-analyzer.service`：

```ini
[Unit]
Description=Redis Key Analyzer
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/data/rediskey_exporter
ExecStart=/usr/local/bin/python3.13 main.py --config /opt/data/rediskey_exporter/config.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl start redis-analyzer
systemctl enable redis-analyzer
systemctl status redis-analyzer
journalctl -u redis-analyzer -f  # 查看运行日志
```
