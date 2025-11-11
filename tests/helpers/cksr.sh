#!/usr/bin/env bash
set -euo pipefail
set -o errtrace

# 颜色与提示增强
RED="\033[31m"; YELLOW="\033[33m"; GREEN="\033[32m"; BOLD="\033[1m"; RESET="\033[0m"
info() { echo -e "${GREEN}$*${RESET}"; }
warn() { echo -e "${YELLOW}$*${RESET}"; }
error() { echo -e "${RED}$*${RESET}"; }

# 统一错误提示：在任何命令失败时打印当前步骤、命令、行号与退出码
CURRENT_STEP=""
_on_err() {
  local status=$?
  local cmd=${BASH_COMMAND}
  local line=${BASH_LINENO[0]:-""}
  echo ""
  error "[失败] 步骤: ${CURRENT_STEP:-未标注}"
  error "[失败] 命令: ${cmd}"
  [[ -n "$line" ]] && error "[失败] 行号: ${line}"
  error "[失败] 退出码: ${status}"
  warn "[提示] 请根据上述错误输出排查并重试。"
}
trap _on_err ERR

# 步骤标记：在关键阶段前调用，使错误提示更清晰
step() {
  CURRENT_STEP="$*"
  echo -e "${BOLD}==== ${CURRENT_STEP} ====${RESET}"
}

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
export -f step
export -f info
export -f warn
export -f error