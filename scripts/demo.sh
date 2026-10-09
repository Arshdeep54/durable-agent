#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$ROOT/.demo-server.pid"
METAFILE="$ROOT/.demo-server.meta"
LOGFILE="$ROOT/.demo-server.log"
BASE_URL="http://127.0.0.1:${PORT:-8080}"
DB="$ROOT/durable-agent.db"

curl_api() {
  local extra=()
  if [[ -n "${DURABLE_AGENT_API_KEY:-}" ]]; then
    extra=(-H "Authorization: Bearer ${DURABLE_AGENT_API_KEY}")
  fi
  curl -sf "${extra[@]}" "$@"
}

server_bin() {
  echo "${ROOT}/target/debug/durable-agent"
}

server_pid() {
  local want pid exe
  want="$(server_bin)"
  for pid in $(pgrep -x durable-agent 2>/dev/null || true); do
    exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
    if [[ "$exe" == "$want" ]]; then
      echo "$pid"
      return 0
    fi
  done
  return 1
}

record_server_pid() {
  local pid
  pid="$(server_pid 2>/dev/null || true)"
  if [[ -n "$pid" ]]; then
    echo "$pid" >"$PIDFILE"
  fi
}

read_meta() {
  local key="$1"
  if [[ ! -f "$METAFILE" ]]; then
    return 1
  fi
  grep -E "^${key}=" "$METAFILE" | head -1 | cut -d= -f2-
}

write_meta() {
  : >"$METAFILE"
  for line in "$@"; do
    echo "$line" >>"$METAFILE"
  done
}

stop_server() {
  if [[ -f "$PIDFILE" ]]; then
    local file_pid
    file_pid="$(<"$PIDFILE")"
    if [[ -n "$file_pid" ]]; then
      kill -TERM "$file_pid" 2>/dev/null || true
    fi
  fi
  local pid
  while read -r pid; do
    [[ -z "$pid" ]] && continue
    kill -TERM "$pid" 2>/dev/null || true
  done < <(
    for pid in $(pgrep -x durable-agent 2>/dev/null || true); do
      exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
      if [[ "$exe" == "$(server_bin)" ]]; then
        echo "$pid"
      fi
    done
  )

  local i
  for i in $(seq 1 40); do
    if ! server_pid >/dev/null 2>&1; then
      rm -f "$PIDFILE" "$METAFILE"
      return 0
    fi
    sleep 0.25
  done
  while read -r pid; do
    [[ -z "$pid" ]] && continue
    kill -KILL "$pid" 2>/dev/null || true
  done < <(
    for pid in $(pgrep -x durable-agent 2>/dev/null || true); do
      exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
      if [[ "$exe" == "$(server_bin)" ]]; then
        echo "$pid"
      fi
    done
  )
  sleep 0.5
  rm -f "$PIDFILE" "$METAFILE"
}

wait_for_health() {
  local i
  for i in $(seq 1 180); do
    if curl -sf "$BASE_URL/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "demo.sh: server did not become healthy (see $LOGFILE)" >&2
  return 1
}

start_server() {
  local mode="$1"
  local dev_tools="$2"
  local dev_kill="$3"

  local -a env_args=()
  env_args+=(-u OPENAI_API_KEY)
  if [[ "$dev_kill" == "1" ]]; then
    env_args+=(DURABLE_AGENT_ALLOW_DEV_KILL=1)
  fi
  case "$mode" in
    normal) ;;
    retry) env_args+=(DEMO_CLASSIFIER_MODE=fail_once) ;;
    timeout)
      env_args+=(DEMO_CLASSIFIER_MODE=slow CLASSIFY_TIMEOUT_SECS=5)
      ;;
    crash_kill)
      env_args+=(DEMO_CLASSIFIER_MODE=slow CLASSIFY_TIMEOUT_SECS=10)
      ;;
    *)
      echo "demo.sh: internal error: unknown mode $mode" >&2
      return 1
      ;;
  esac

  local -a cargo_args=(run)
  if [[ "$dev_tools" == "1" ]]; then
    cargo_args+=(--features dev-tools)
  fi

  (
    cd "$ROOT"
    exec env "${env_args[@]}" cargo "${cargo_args[@]}"
  ) >>"$LOGFILE" 2>&1 &
  write_meta \
    "mode=$mode" \
    "dev_tools=$dev_tools" \
    "dev_kill=$dev_kill"
  wait_for_health
  record_server_pid
}

ensure_server() {
  local mode="$1"
  local dev_tools="$2"
  local dev_kill="$3"
  if server_pid >/dev/null 2>&1; then
    return 0
  fi
  rm -f "$LOGFILE"
  start_server "$mode" "$dev_tools" "$dev_kill"
}

new_workflow_id() {
  echo "demo-$(date +%s)-${RANDOM}"
}

create_and_run_workflow() {
  local wf_id="$1"
  local ticket
  ticket=$(printf '{"id":"%s","customer_id":"demo@example.com","subject":"Demo ticket","body":"Need help with my account login"}' "$wf_id")
  curl_api -X POST "$BASE_URL/workflows" \
    -H "Content-Type: application/json" \
    -d "$ticket" >/dev/null
  curl_api -X POST "$BASE_URL/workflows/${wf_id}/run" >/dev/null
  echo "$wf_id"
}

cmd_reset() {
  stop_server
  rm -f "$DB" "${DB}-wal" "${DB}-shm" "$LOGFILE"
  echo "demo reset: stopped server (if any) and removed database"
}

cmd_run() {
  local mode="${1:-}"
  case "$mode" in
    normal | retry | timeout) ;;
    *)
      echo "usage: $0 run normal|retry|timeout" >&2
      exit 1
      ;;
  esac
  ensure_server "$mode" 0 0
  local wf_id
  wf_id="$(new_workflow_id)"
  wf_id="$(create_and_run_workflow "$wf_id")"
  echo "workflow_id=$wf_id"
  echo "progress: curl -s ${BASE_URL}/workflows/${wf_id}/events"
}

events_json() {
  local wf_id="$1"
  curl_api "$BASE_URL/workflows/${wf_id}/events"
}

wait_for_classify_started() {
  local wf_id="$1"
  local events i
  for i in $(seq 1 300); do
    events="$(events_json "$wf_id")"
    if echo "$events" | grep -qE '"StepCompleted":\{[^}]*"step_index":1'; then
      echo "demo.sh: ClassifyTicket finished before crash window" >&2
      return 1
    fi
    if echo "$events" | grep -qE '"StepStarted":\{[^}]*"step_index":1'; then
      return 0
    fi
    sleep 0.05
  done
  echo "demo.sh: ClassifyTicket (step 1) did not start in time" >&2
  return 1
}

cmd_crash() {
  local had_server=0
  if server_pid >/dev/null 2>&1; then
    had_server=1
    local dt dk
    dt="$(read_meta dev_tools || echo 0)"
    dk="$(read_meta dev_kill || echo 0)"
    if [[ "$dt" != "1" || "$dk" != "1" ]]; then
      echo "demo.sh crash: server is running but was not started with dev-tools and DURABLE_AGENT_ALLOW_DEV_KILL=1" >&2
      echo "  run: $0 reset && $0 crash   (or stop the server and let crash start one)" >&2
      exit 1
    fi
  else
    rm -f "$LOGFILE"
    start_server crash_kill 1 1
  fi

  local wf_id
  wf_id="$(new_workflow_id)"
  wf_id="$(create_and_run_workflow "$wf_id")"
  wait_for_classify_started "$wf_id"

  local pid
  if ! pid="$(server_pid)"; then
    echo "demo.sh crash: could not find durable-agent process" >&2
    exit 1
  fi
  echo "$pid" >"$PIDFILE"
  curl -sf -X POST "$BASE_URL/dev/kill" >/dev/null || true
  sleep 0.5
  if kill -0 "$pid" 2>/dev/null; then
    echo "demo.sh crash: process $pid still alive after /dev/kill" >&2
    exit 1
  fi
  rm -f "$PIDFILE"

  start_server normal 1 1
  echo "demo.sh crash: waiting for worker lease expiry before recovery restart..."
  sleep 32
  stop_server
  start_server normal 1 1
  echo "workflow_id=$wf_id"
  echo "recovery: curl -s ${BASE_URL}/workflows/${wf_id}/events | grep WorkerRecovered"
  echo "completion: curl -s ${BASE_URL}/workflows/${wf_id} | grep '\"status\":\"completed\"'"
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    reset) cmd_reset ;;
    run) shift; cmd_run "${1:-}" ;;
    crash) cmd_crash ;;
    *)
      echo "usage: $0 reset|run <normal|retry|timeout>|crash" >&2
      exit 1
      ;;
  esac
}

main "${@:-}"
