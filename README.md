# CKSR - ClickHouse StarRocks 数据同步工具

CKSR是一个用于ClickHouse和StarRocks之间数据同步的工具，通过解析CREATE TABLE DDL语句，自动生成兼容性ALTER TABLE语句和CREATE VIEW语句，实现两个数据库系统之间的数据联合查询。

## 功能特性

- 🔄 **自动表结构导出**: 从ClickHouse和StarRocks数据库导出所有表的CREATE TABLE语句
- 🔧 **智能兼容性处理**: 自动生成ClickHouse的ALTER TABLE语句解决兼容性问题
- 📊 **视图自动创建**: 在StarRocks中创建包含两张表所有字段的UNION ALL视图
- 🔗 **Catalog管理**: 自动创建和管理StarRocks的ClickHouse JDBC Catalog
- 📁 **文件管理**: 将导出的DDL语句按数据库分类存储到临时目录
- ⚡ **批量执行**: 支持批量执行生成的SQL语句
- 🐳 **容器化部署**: 提供完整的Docker和Kubernetes部署方案
- 🔄 **CI/CD集成**: 包含完整的GitHub Actions流水线

## 快速开始

### 1. 配置文件

复制配置文件模板并修改配置：

```bash
cp config.example.json config.json
```

修改 `config.json` 中的数据库连接信息：

```json
{
  "clickhouse": {
    "host": "your-clickhouse-host",
    "port": 9000,
    "username": "default",
    "password": "your-password",
    "database": "your-database"
  },
  "starrocks": {
    "host": "your-starrocks-host",
    "port": 9030,
    "username": "root",
    "password": "your-password",
    "database": "your-database"
  },
  "temp_dir": "./temp",
  "driver_url": "http://your-host/clickhouse-jdbc-0.4.6-all.jar"
}
```

### 2. 本地运行

```bash
# 安装依赖
go mod download

# 编译运行
go build -o cksr .
./cksr config.json
```

### 3. Docker运行

```bash
# 构建镜像
docker build -t cksr:latest .

# 运行容器
docker run -v $(pwd)/config.json:/etc/cksr/config.json cksr:latest
```

## Kubernetes部署

### 1. 配置部署

```bash
# 创建ConfigMap
kubectl apply -f k8s/configmap.yaml

# 创建Secret（可选，用于存储敏感信息）
kubectl apply -f k8s/secret.yaml
```

### 2. 一次性任务

```bash
# 部署Job
kubectl apply -f k8s/job.yaml

# 查看执行状态
kubectl get jobs
kubectl logs job/cksr-job
```

### 3. 定时任务

```bash
# 部署CronJob（每天凌晨2点执行）
kubectl apply -f k8s/cronjob.yaml

# 查看定时任务状态
kubectl get cronjobs
kubectl get jobs -l app=cksr
```

## 工作流程

1. **配置加载**: 从配置文件加载ClickHouse和StarRocks连接信息
2. **表结构导出**: 分别从两个数据库导出所有表的CREATE TABLE语句
3. **文件存储**: 将导出的DDL语句按数据库分类存储到临时目录
4. **Catalog创建**: 在StarRocks中创建ClickHouse JDBC Catalog
5. **表结构解析**: 解析共同表的结构，识别字段差异
6. **SQL生成**: 生成ClickHouse的ALTER TABLE语句和StarRocks的CREATE VIEW语句
7. **批量执行**: 在相应数据库中执行生成的SQL语句

## 项目结构

```
cksr/
├── builder/              # SQL构建器
│   ├── ckAddColumnBuilder.go
│   ├── ckFieldConverter.go
│   └── viewBuilder.go
├── config/               # 配置管理
│   └── config.go
├── database/             # 数据库操作
│   └── database.go
├── fileops/              # 文件操作
│   └── fileops.go
├── parser/               # DDL解析器
│   ├── comm.go
│   ├── parser.go
│   └── table.go
├── k8s/                  # Kubernetes配置
│   ├── configmap.yaml
│   ├── cronjob.yaml
│   ├── job.yaml
│   └── secret.yaml
├── .github/workflows/    # CI/CD流水线
│   └── ci-cd.yml
├── Dockerfile
├── config.example.json
├── go.mod
├── go.sum
├── main.go
└── README.md
```

## 依赖项

- Go 1.23.5+
- ClickHouse Go驱动: `github.com/ClickHouse/clickhouse-go/v2`
- MySQL驱动: `github.com/go-sql-driver/mysql`

## 环境要求

- ClickHouse 21.0+
- StarRocks 2.0+
- Kubernetes 1.20+ (用于容器化部署)

## 配置说明

### 数据库配置

- `host`: 数据库主机地址
- `port`: 数据库端口
- `username`: 用户名
- `password`: 密码
- `database`: 数据库名称

### 其他配置

- `temp_dir`: 临时文件存储目录
- `driver_url`: ClickHouse JDBC驱动下载地址

## 故障排除

### 常见问题

1. **连接失败**: 检查数据库连接配置和网络连通性
2. **权限不足**: 确保数据库用户具有相应的读写权限
3. **驱动下载失败**: 检查driver_url是否可访问

### 日志查看

```bash
# Kubernetes环境
kubectl logs job/cksr-job
kubectl logs cronjob/cksr-cronjob

# Docker环境
docker logs <container-id>
```

## 贡献指南

1. Fork本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建Pull Request

## 许可证

本项目采用MIT许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 支持

如有问题或建议，请创建Issue或联系维护者。