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

# 表不存在断言
assert_sr_table_not_exists() {
  local name="$1"; local msg="${2:-表仍存在: ${name}}"
  if sr_table_exists "${name}"; then
    _assert_fail "${msg}"; return 0
  else
    return 0
  fi
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
export -f assert_sr_table_not_exists

# 断言收尾（兼容占位）：严格模式下无需汇总，空操作即可
asserts_finalize() { return 0; }

export -f asserts_finalize

####################################
# 进一步的断言增强（查询/结构/行数）
####################################

# DESCRIBE 中包含指定列
sr_describe_contains() {
  local name="$1"; local column="$2"
  local out
  out=$(mysql_query "DESC \`${name}\`" || true)
  echo "$out" | awk '{print $1}' | grep -Fxq "$column"
}

assert_sr_describe_contains() {
  local name="$1"; local column="$2"; local msg="${3:-视图 \`${name}\` 不包含列: ${column}}"
  if sr_describe_contains "$name" "$column"; then
    return 0
  else
    _assert_fail "$msg"; return 0
  fi
}

# 运行一个最小查询以确认视图可查询
assert_sr_view_select_ok() {
  local name="$1"; local msg="${2:-视图 \`${name}\` 查询失败}"
  if mysql_query "SELECT 1 FROM \`${name}\` LIMIT 1" >/dev/null 2>&1; then
    return 0
  else
    _assert_fail "$msg"; return 0
  fi
}

# 统计视图行数并断言 >= min
assert_sr_view_row_count_ge() {
  local name="$1"; local min="$2"; local msg="${3:-视图 \`${name}\` 行数小于 ${min}}"
  local c
  c=$(mysql_query "SELECT COUNT(*) FROM \`${name}\`" || echo 0)
  if [[ "$c" =~ ^[0-9]+$ ]] && (( c >= min )); then
    return 0
  else
    _assert_fail "$msg"; return 0
  fi
}

# 基础 SR 表（视图名+后缀）有数据
assert_sr_base_has_rows() {
  local view="$1"; local msg="${2:-基础表 \`${view}${SR_SUFFIX}\` 无数据}"
  if sr_has_rows "$view"; then
    return 0
  else
    _assert_fail "$msg"; return 0
  fi
}

# 断言某命令失败且输出包含预期内容（expected 可为正则）
assert_cmd_fail_contains() {
  local cmd="$1"; local expected="$2"; local msg="${3:-命令失败输出未包含预期内容: ${expected}}"
  local out status
  set +e
  out=$(eval "$cmd" 2>&1)
  status=$?
  set -e
  if [[ $status -eq 0 ]]; then
    _assert_fail "预期失败但实际成功：${cmd}"; return 0
  fi
  echo "$out" | grep -Eiq "$expected" || _assert_fail "${msg}。实际输出: ${out}"
  return 0
}

export -f sr_describe_contains
export -f assert_sr_describe_contains
export -f assert_sr_view_select_ok
export -f assert_sr_view_row_count_ge
export -f assert_sr_base_has_rows
export -f assert_cmd_fail_contains