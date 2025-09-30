# Makefile for CKSR project

# 变量定义
APP_NAME := cksr
VERSION := $(shell git describe --tags --always --dirty)
BUILD_TIME := $(shell date +%Y-%m-%d_%H:%M:%S)
GO_VERSION := $(shell go version | awk '{print $$3}')

# Docker相关变量
DOCKER_REGISTRY := ghcr.io
DOCKER_IMAGE := $(DOCKER_REGISTRY)/$(shell echo $(shell git config --get remote.origin.url) | sed 's/.*github.com[:/]\([^.]*\).*/\1/' | tr '[:upper:]' '[:lower:]')
DOCKER_TAG := $(VERSION)

# Kubernetes相关变量
NAMESPACE := default
KUBECONFIG_FILE := ~/.kube/config

# 构建标志
LDFLAGS := -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GoVersion=$(GO_VERSION)

.PHONY: help
help: ## 显示帮助信息
	@echo "CKSR - ClickHouse StarRocks 数据同步工具"
	@echo ""
	@echo "可用命令:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: deps
deps: ## 安装依赖
	go mod download
	go mod tidy

.PHONY: build
build: deps ## 构建应用
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(APP_NAME) .

.PHONY: build-local
build-local: deps ## 构建本地应用
	go build -ldflags "$(LDFLAGS)" -o bin/$(APP_NAME) .

.PHONY: test
test: ## 运行测试
	go test -v ./...

.PHONY: fmt
fmt: ## 格式化代码
	go fmt ./...

.PHONY: vet
vet: ## 运行go vet
	go vet ./...

.PHONY: lint
lint: ## 运行代码检查
	golangci-lint run

.PHONY: clean
clean: ## 清理构建文件
	rm -rf bin/
	rm -rf temp/
	docker rmi $(DOCKER_IMAGE):$(DOCKER_TAG) 2>/dev/null || true

.PHONY: docker-build
docker-build: ## 构建Docker镜像
	docker build -t $(APP_NAME):latest .
	docker tag $(APP_NAME):latest $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker tag $(APP_NAME):latest $(DOCKER_IMAGE):latest

.PHONY: docker-push
docker-push: docker-build ## 推送Docker镜像
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

.PHONY: docker-run
docker-run: ## 运行Docker容器
	docker run --rm -v $(PWD)/config.json:/etc/cksr/config.json $(APP_NAME):latest

.PHONY: k8s-deploy-config
k8s-deploy-config: ## 部署Kubernetes配置
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/secret.yaml

.PHONY: k8s-deploy-job
k8s-deploy-job: k8s-deploy-config ## 部署Kubernetes Job
	kubectl apply -f k8s/job.yaml

.PHONY: k8s-deploy-cronjob
k8s-deploy-cronjob: k8s-deploy-config ## 部署Kubernetes CronJob
	kubectl apply -f k8s/cronjob.yaml

.PHONY: k8s-logs
k8s-logs: ## 查看Kubernetes日志
	kubectl logs -l app=$(APP_NAME) --tail=100

.PHONY: k8s-status
k8s-status: ## 查看Kubernetes状态
	kubectl get jobs,cronjobs,pods -l app=$(APP_NAME)

.PHONY: k8s-delete
k8s-delete: ## 删除Kubernetes资源
	kubectl delete jobs,cronjobs -l app=$(APP_NAME)
	kubectl delete configmap cksr-config
	kubectl delete secret cksr-secret

.PHONY: run
run: build-local ## 运行应用
	./bin/$(APP_NAME) config.json

.PHONY: dev
dev: ## 开发模式运行
	go run . config.json

.PHONY: install
install: build ## 安装应用到系统
	sudo cp bin/$(APP_NAME) /usr/local/bin/

.PHONY: uninstall
uninstall: ## 从系统卸载应用
	sudo rm -f /usr/local/bin/$(APP_NAME)

.PHONY: release
release: test docker-push ## 发布版本
	@echo "Released version $(VERSION)"

.PHONY: all
all: fmt vet test build docker-build ## 执行所有构建步骤

# 显示版本信息
.PHONY: version
version: ## 显示版本信息
	@echo "App Name: $(APP_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(GO_VERSION)"
	@echo "Docker Image: $(DOCKER_IMAGE):$(DOCKER_TAG)"