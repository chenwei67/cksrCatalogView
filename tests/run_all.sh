#!/usr/bin/env bash
set -euo pipefail

# 串行执行所有用例：自动读取 tests/cases 目录并按文件名排序执行

run_case() {
  local script="$1"
  echo "===================="
  echo "[RUN] $script"
  if bash "$script"; then
    echo "[OK]  $script"
  else
    echo "[FAIL] $script"
    return 1
  fi
}

CASES_DIR="tests/cases"
echo "[INFO] 自动发现并执行 ${CASES_DIR} 下的用例脚本"
found_any=false
while IFS= read -r script; do
  found_any=true
  run_case "$script"
done < <(find "$CASES_DIR" -maxdepth 1 -type f -name "*.sh" | sort)

if [[ "$found_any" != true ]]; then
  echo "[WARN] 未在 ${CASES_DIR} 下发现任何 .sh 用例脚本"
else
  echo "[完成] 所有用例脚本已按序执行"
fi