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

# ----------------------
# 安全断言（失败不退出，仅记录）
# ----------------------
ASSERT_FAIL_COUNT=${ASSERT_FAIL_COUNT:-0}

_assert_fail() {
  local msg="$1"
  echo "[断言失败] ${msg}"
  ASSERT_FAIL_COUNT=$((ASSERT_FAIL_COUNT + 1))
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

# 断言收尾：若存在断言失败，则返回非零退出码
asserts_finalize() {
  local count="${ASSERT_FAIL_COUNT:-0}"
  if [[ "$count" -gt 0 ]]; then
    echo "[断言汇总] 失败次数: ${count}"
    return 1
  else
    echo "[断言汇总] 全部通过"
    return 0
  fi
}

export -f asserts_finalize