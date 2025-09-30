# 使用官方Go镜像作为构建环境
FROM golang:1.23.5-alpine AS builder

# 设置工作目录
WORKDIR /app

# 安装必要的包
RUN apk add --no-cache git ca-certificates tzdata

# 复制go mod文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cksr .

# 使用轻量级的alpine镜像作为运行环境
FROM alpine:latest

# 安装ca-certificates用于HTTPS请求
RUN apk --no-cache add ca-certificates tzdata

# 设置工作目录
WORKDIR /root/

# 从构建阶段复制二进制文件
COPY --from=builder /app/cksr .

# 创建配置目录
RUN mkdir -p /etc/cksr

# 设置时区
ENV TZ=Asia/Shanghai

# 暴露端口（如果需要）
# EXPOSE 8080

# 运行应用
ENTRYPOINT ["./cksr"]
CMD ["/etc/cksr/config.json"]