#!/usr/bin/env bash
set -euo pipefail

# 用例：一次性更新参数缺失/非法
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
first_sql=$(ls -1 "${SQL_DIR}"/*.sql 2>/dev/null | head -n1 || true)
if [[ -z "${first_sql}" ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生视图名"; exit 1;
fi
BASE_NAME="$(basename "${first_sql}")"; BASE_NAME="${BASE_NAME%.sql}"

pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "执行init,预期成功"
if cksr init --config ./config.json; then
  info "[断言] init成功，符合预期"
else
  echo "预期init成功，但失败";exit 1;
fi

step "执行A 缺少必要参数（预期失败）"
if cksr update --config ./config.json; then
  echo "预期失败但实际成功：未校验缺少参数"; exit 1;
else
  info "[断言A] 失败符合预期，缺少 --pair 或 --table"
fi

step "执行B 分区类型不匹配（为 datetime 列传入未加引号数值，预期失败）"
if cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition 20250101; then
  echo "预期失败但实际成功：未校验分区类型"; exit 1;
else
  info "[断言B] 失败符合预期，ALTER VIEW 执行报错"
fi

:
info "[通过] 14_update_param_errors（预期失败用例）"