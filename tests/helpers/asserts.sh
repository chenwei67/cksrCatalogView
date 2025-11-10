#!/usr/bin/env bash
set -euo pipefail

# 依赖：在此之前已 source tests/helpers/config.sh ./config.json

sr_table_exists() {
  local name="$1"
  local out
  out=$(mysql_query "SHOW TABLES LIKE '${name}';" || true)
  [[ "$out" == "$name" ]]
}

sr_view_exists() {
  local name="$1"
  local out
  out=$(mysql_query "SHOW FULL TABLES WHERE Table_type='VIEW';" || true)
  echo "$out" | awk '{print $1}' | grep -Fxq "$name"
}

sr_show_create_view_contains() {
  local name="$1"; shift
  local needle="$*"
  local out
  out=$(mysql_exec "SHOW CREATE VIEW \`${name}\`" || true)
  echo "$out" | tr -s ' ' | grep -Fiq "$needle"
}

sr_drop_table_if_exists() {
  local name="$1"
  mysql_exec "DROP TABLE IF EXISTS \`${name}\`"
}

# 删除视图（若存在）
sr_drop_view_if_exists() {
  local name="$1"
  mysql_exec "DROP VIEW IF EXISTS \`${name}\`"
}