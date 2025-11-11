#!/usr/bin/env bash
set -euo pipefail

# 串行执行所有用例，不实际运行时可作为参考入口

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

run_case tests/cases/01_init_create_view.sh
run_case tests/cases/03_rollback.sh
run_case tests/cases/11_idempotent_init.sh
run_case tests/cases/02a_update_with_data.sh
run_case tests/cases/02b_update_without_data.sh
run_case tests/cases/12_invalid_mapping.sh || true
run_case tests/cases/13_rename_conflict.sh || true
run_case tests/cases/14_update_param_errors.sh || true

echo "[完成] 所有用例脚本已准备好，可在 Linux 环境运行"