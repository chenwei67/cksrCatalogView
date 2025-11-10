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

# 记录最近一次 cksr 执行的退出码
LAST_CKSR_STATUS=0

# 安全执行：不因非零退出码中断当前脚本，记录退出码到 LAST_CKSR_STATUS
cksr_safe() {
  set +e
  cksr "$@"
  local status=$?
  set -e
  LAST_CKSR_STATUS=$status
  if [[ $status -ne 0 ]]; then
    echo "[警告] cksr 命令失败(退出码=${status})：cksr $@"
  fi
  return 0
}

# 是否继续执行断言（最近一次 cksr 命令成功时返回0）
should_assert() {
  [[ "${LAST_CKSR_STATUS:-0}" -eq 0 ]]
}

export -f cksr
export -f cksr_safe
export -f should_assert