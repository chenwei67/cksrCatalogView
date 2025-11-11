SHELL := /bin/bash

# 配置可变参数
DOCKER_BUILDKIT ?= 1
DIST_DIR := dist
OUTDIR := $(DIST_DIR)/linux-amd64
BIN := $(OUTDIR)/cksr

.PHONY: help export test run-case clean

help:
	@echo "可用目标:"
	@echo "  export  通过 Docker 导出 linux/amd64 二进制到 $(OUTDIR)"
	@echo "  test    使用导出的二进制在容器外运行测试（需 jq 和 mysql 客户端）"
	@echo "  clean   清理构建产物目录 $(DIST_DIR)"

# 使用 artifact 阶段导出（固定为 linux/amd64）
export:
	@mkdir -p $(OUTDIR)
	@echo "==> Exporting linux/amd64 binary to $(OUTDIR)"
	@DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) docker build --target artifact \
		--build-arg TARGETOS=linux --build-arg TARGETARCH=amd64 \
		-o type=local,dest=$(OUTDIR) .
	@echo "==> Binary: $(BIN)"
	@CKSR_BIN="$(BIN)"


# 使用导出的二进制运行测试（容器外），自动先导出
test: export
	@command -v jq >/dev/null 2>&1 || { echo "错误: 需要安装 jq"; exit 1; }
	@command -v mysql >/dev/null 2>&1 || { echo "错误: 需要安装 mysql 客户端"; exit 1; }
	@if [[ ! -f "$(BIN)" ]]; then echo "错误: 未找到二进制 $(BIN)"; exit 1; fi
	@CKSR_BIN="$(BIN)" bash tests/run_all.sh

# 运行指定用例脚本：传入 CASE=脚本路径
run-case: export
	@command -v jq >/dev/null 2>&1 || { echo "错误: 需要安装 jq"; exit 1; }
	@command -v mysql >/dev/null 2>&1 || { echo "错误: 需要安装 mysql 客户端"; exit 1; }
	@if [[ -z "$(CASE)" ]]; then echo "错误: 需要传 CASE=用例脚本路径，例如 CASE=tests/cases/02a_update_with_data.sh"; exit 1; fi
	@if [[ ! -f "$(CASE)" ]]; then echo "错误: 用例文件不存在: $(CASE)"; exit 1; fi
	@if [[ ! -f "$(BIN)" ]]; then echo "错误: 未找到二进制 $(BIN)"; exit 1; fi
	@echo "==> 使用二进制: $(BIN) 运行用例: $(CASE)"
	@CKSR_BIN="$(BIN)" bash "$(CASE)"

clean:
	@rm -rf $(DIST_DIR)