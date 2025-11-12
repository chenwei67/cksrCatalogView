#!/usr/bin/env bash
set -Eeuo pipefail

# 清理与准备助手：要求在每个用例执行前进行完整清理（先 rollback，再删除所有 SR 表），
# 并在需要时创建 temp/sqls 下的所有表。
#
# 依赖：需在 source 本文件前先 source tests/helpers/config.sh ./config.json 与 tests/helpers/cksr.sh

# 删除 SR 数据库下的所有“表”（不含视图）
sr_drop_all_tables() {
  step "清理 删除所有 SR 表"
  local rows
  rows=$(mysql_query "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='${SR_DB}' AND TABLE_TYPE!='VIEW';" || true)
  if [[ -z "$rows" ]]; then
    info "未发现需要删除的 SR 表，跳过"
    return 0
  fi
  while IFS=$'\t' read -r tname; do
    [[ -z "$tname" ]] && continue
    info "DROP TABLE IF EXISTS \`$tname\`"
    mysql_exec "DROP TABLE IF EXISTS \`$tname\`"
  done <<< "$rows"
}

# 前置：完整清理（先 rollback，再删除所有 SR 表）
pre_case_cleanup() {
  step "清理 执行 rollback"
  cksr rollback --config ./config.json
  sr_drop_all_tables
}

# 后置：为保持环境整洁，重复执行一次完整清理
post_case_cleanup() {
  step "收尾 回滚并删除所有 SR 表"
  cksr rollback --config ./config.json
  sr_drop_all_tables
}

# 若 temp/sqls 目录存在并包含 .sql 文件，则执行建表
ensure_temp_sql_tables() {
  local sql_dir="${1:-${TEMP_DIR}/sqls}"
  if [[ -d "$sql_dir" ]] && ls -1 "$sql_dir"/*.sql >/dev/null 2>&1; then
    step "准备 执行建表：${sql_dir}"
    ./execute_sql.sh ./config.json "$sql_dir"
  else
    info "未检测到 ${sql_dir} 下的 .sql 文件，略过建表"
  fi
}

export -f sr_drop_all_tables
export -f pre_case_cleanup
export -f post_case_cleanup
export -f ensure_temp_sql_tables