# syntax=docker/dockerfile:1.4
# 使用官方Go镜像作为构建环境
FROM mirrors.sangfor.com/golang:1.25 AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

ENV GO111MODULE=on
ARG GOPROXY=http://mirrors.sangfor.org/nexus/repository/go-proxy-group
ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=off

# 设置工作目录
WORKDIR /app

# 复制go mod文件
COPY go.mod go.sum ./

# 下载依赖
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# 复制源代码
COPY . .

# 构建应用
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o cksr .

# 使用轻量级的alpine镜像作为运行环境
FROM docker.sangfor.com/xaasos-public/alpine-base:3.18.3

# 设置工作目录
WORKDIR /root/

# 从构建阶段复制二进制文件
COPY --from=builder /app/cksr .

# 创建配置目录
RUN mkdir -p /etc/cksr

# 设置时区
ENV TZ=Asia/Shanghai

# 运行应用
ENTRYPOINT ["./cksr"]
CMD ["/etc/cksr/config.json"]

# 仅用于导出构建产物到宿主机（不用于运行）
FROM scratch AS artifact
COPY --from=builder /app/cksr /cksr