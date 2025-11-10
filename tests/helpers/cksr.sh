#!/usr/bin/env bash
set -euo pipefail

# 统一命令入口：优先使用导出的二进制 CKSR_BIN，其次使用 go run .
# 用法： cksr <subcommand> [args...]

cksr() {
  if [[ -n "${CKSR_BIN:-}" ]]; then
    # 显式二进制
    "${CKSR_BIN}" "$@"
  else
    # 默认 go run
    go run . "$@"
  fi
}

export -f cksr