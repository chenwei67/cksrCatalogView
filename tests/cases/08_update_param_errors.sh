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
outA=$(cksr update --config ./config.json 2>&1 || true)
if echo "$outA" | grep -Fq "CONFIG_ERROR: 必须提供 --pair"; then
  info "[断言A] 错误信息包含: CONFIG_ERROR: 必须提供 --pair"
else
  echo "预期失败且包含缺少 --pair，但输出不符：$outA"; exit 1;
fi

step "执行B 分区类型不匹配（为 datetime 列传入未加引号数值，预期失败）"
outB=$(cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition 20250101 2>&1 || true)
if echo "$outB" | grep -Eiq "执行ALTER VIEW语句失败|构建ALTER VIEW SQL失败"; then
  info "[断言B] 错误信息包含 ALTER VIEW 失败"
else
  echo "预期失败但实际输出不包含预期错误信息：$outB"; exit 1;
fi

:
info "[通过] 08_update_param_errors（预期失败用例）"