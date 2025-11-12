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
  # 安全删除：仅当对象确认为视图时才执行删除，避免对象是表时的 1347 错误
  if sr_view_exists "${name}"; then
    mysql_exec "DROP VIEW IF EXISTS \`${name}\`"
  else
    # 若对象不存在或是表，则不进行 DROP VIEW
    return 0
  fi
}

# ----------------------
# 严格断言：失败立即退出
# ----------------------
_assert_fail() {
  local msg="$1"
  echo "[断言失败] ${msg}" >&2
  exit 1
}

# 表存在断言
assert_sr_table_exists() {
  local name="$1"; local msg="${2:-表不存在: ${name}}"
  if sr_table_exists "${name}"; then
    return 0
  else
    _assert_fail "${msg}"
    return 0
  fi
}

# 视图存在断言
assert_sr_view_exists() {
  local name="$1"; local msg="${2:-视图不存在: ${name}}"
  if sr_view_exists "${name}"; then
    return 0
  else
    _assert_fail "${msg}"
    return 0
  fi
}

# 视图不存在断言
assert_sr_view_not_exists() {
  local name="$1"; local msg="${2:-视图仍存在: ${name}}"
  if sr_view_exists "${name}"; then
    _assert_fail "${msg}"
    return 0
  else
    return 0
  fi
}

# 视图定义包含断言
assert_sr_view_contains() {
  local name="$1"; shift
  local needle="$1"; shift || true
  local msg="${1:-视图 \`${name}\` 定义未包含: ${needle}}"
  if sr_show_create_view_contains "${name}" "${needle}"; then
    return 0
  else
    _assert_fail "${msg}"
    return 0
  fi
}

export -f assert_sr_table_exists
export -f assert_sr_view_exists
export -f assert_sr_view_not_exists
export -f assert_sr_view_contains

# 断言收尾（兼容占位）：严格模式下无需汇总，空操作即可
asserts_finalize() { return 0; }

export -f asserts_finalize