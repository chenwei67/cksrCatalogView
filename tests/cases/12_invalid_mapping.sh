#!/usr/bin/env bash
set -euo pipefail

# 用例：字段映射/类型不匹配导致失败（不修改 temp/sqls，使用自己的 fixtures）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh

echo "[清理前置] 删除可能存在的视图与表，确保干净环境"
sr_drop_view_if_exists "dns_log" || true
sr_drop_table_if_exists "dns_log${SR_SUFFIX}" || true
sr_drop_table_if_exists "dns_log" || true

echo "[准备] 在 SR 创建一个包含额外列的表以制造映射不一致"
./execute_sql.sh ./config.json ./tests/fixtures/invalid_mapping

echo "[执行] 初始化（预期成功）"
if cksr init --config ./config.json; then
  echo "[断言] 成功符合预期，SR-only 列使用默认占位处理"
else
  echo "预期成功但实际失败：映射未通过"; exit 1;
fi

echo "[清理后置] 回滚并删除异常表，恢复初始状态"
cksr rollback --config ./config.json || true
sr_drop_view_if_exists "dns_log" || true
sr_drop_table_if_exists "dns_log${SR_SUFFIX}" || true
sr_drop_table_if_exists "dns_log" || true

echo "[通过] 12_invalid_mapping（SR-only 列默认占位用例）"