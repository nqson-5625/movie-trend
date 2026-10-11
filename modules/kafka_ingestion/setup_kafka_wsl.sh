#!/usr/bin/env bash
# Install/check Apache Kafka 4.3.1 KRaft single-node on WSL2 Ubuntu 24.04.
# Safe to re-run: NEVER wipes or reformats existing storage.
set -Eeuo pipefail

KAFKA_VERSION="4.3.1"
ARCHIVE="kafka_2.13-${KAFKA_VERSION}.tgz"
RELEASE_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}"
# Pinned digest from the official Apache Kafka 4.3.1 release index (.sha512).
PINNED_SHA512="c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c"
TARGET="/opt/kafka_2.13-${KAFKA_VERSION}"
KAFKA_HOME="/opt/kafka"
LOCAL_ROOT="${HOME}/kafka-local"
CONFIG="${LOCAL_ROOT}/config/server.properties"
DATA="${LOCAL_ROOT}/data"
LOG_DIR="${LOCAL_ROOT}/logs"
CLUSTER_ID_FILE="${LOCAL_ROOT}/cluster.id"
DOWNLOADS="${HOME}/downloads/kafka"
BOOTSTRAP="127.0.0.1:9092"
MODE="${1:-setup}"

info() { printf '[INFO] %s\n' "$*"; }
pass() { printf '[PASS] %s\n' "$*"; }
die() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'HELP'
Kafka 4.3.1 / WSL2 Ubuntu 24.04 — one-time setup for local development

Usage:
  bash modules/kafka_ingestion/setup_kafka_wsl.sh
  bash modules/kafka_ingestion/setup_kafka_wsl.sh --check
  bash modules/kafka_ingestion/setup_kafka_wsl.sh --help

setup: install missing prerequisites, download+verify Kafka, create local
       KRaft config, format storage only if EMPTY, start broker, verify quorum.
--check: read-only checks of installed Kafka and live broker/quorum.

Never deletes Kafka data or topics; never reformats formatted storage.
Does not install Python dependencies, create Kafka topics or enable autostart.
HELP
}

case "$MODE" in
  setup) ;;
  --check) ;;
  --help|-h) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

require_platform() {
  [[ "${EUID}" -ne 0 ]] || die 'Run as your normal Ubuntu user, NOT with sudo.'
  [[ -n "${HOME}" && -d "${HOME}" && -w "${HOME}" ]] || die 'HOME must be writable.'
  [[ "${HOME}" != /mnt/* ]] || die 'HOME must be on the Linux filesystem, not /mnt/c.'
  local script_dir
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
  [[ "$script_dir" != /mnt/* ]] || die 'The Git project must be on the Linux filesystem, not /mnt/c.'
  [[ -r /etc/os-release ]] || die 'Cannot determine Ubuntu version.'
  # shellcheck disable=SC1091
  . /etc/os-release
  [[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 ]] || die "Supported environment: Ubuntu 24.04; found ${PRETTY_NAME:-unknown}."
  grep -qiE 'microsoft|wsl' /proc/sys/kernel/osrelease || die 'This bootstrapper targets WSL2 Ubuntu.'
  [[ "$(uname -m)" == x86_64 || "$(uname -m)" == aarch64 ]] || die 'CPU architecture must be x86_64 or aarch64.'
}

setup_prerequisites() {
  command -v sudo >/dev/null || die 'sudo missing; please configure Ubuntu sudo first.'
  local pkgs=()
  command -v curl >/dev/null || pkgs+=(curl)
  command -v tar >/dev/null || pkgs+=(tar)
  command -v sha512sum >/dev/null || pkgs+=(coreutils)
  command -v timeout >/dev/null || pkgs+=(coreutils)
  command -v ss >/dev/null || pkgs+=(iproute2)
  command -v python3 >/dev/null || pkgs+=(python3)
  [[ -f /etc/ssl/certs/ca-certificates.crt ]] || pkgs+=(ca-certificates)
  local distro_arch
  distro_arch="$(dpkg --print-architecture)"
  case "$distro_arch" in
    amd64|arm64) ;;
    *) die "Unsupported Debian architecture: ${distro_arch}" ;;
  esac
  export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-${distro_arch}"
  if [[ ! -x "${JAVA_HOME}/bin/java" || ! -x "${JAVA_HOME}/bin/javac" ]]; then
    pkgs+=(openjdk-17-jdk-headless)
  fi
  if ((${#pkgs[@]})); then
    info "Installing missing Ubuntu packages: ${pkgs[*]}"
    sudo apt-get update
    sudo apt-get install -y "${pkgs[@]}"
  fi
  [[ -x "${JAVA_HOME}/bin/java" ]] || die "Java 17 missing at ${JAVA_HOME}"
  [[ -x "${JAVA_HOME}/bin/javac" ]] || die 'JDK compiler missing.'
  "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q 'version "17\.' || die 'Expected Java 17.'
  pass "JDK17 at ${JAVA_HOME}"
}

check_prerequisites() {
  for cmd in curl tar sha512sum timeout ss python3; do
    command -v "$cmd" >/dev/null || die "$cmd missing. Run this script without --check first."
  done
  local distro_arch
  distro_arch="$(dpkg --print-architecture)"
  export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-${distro_arch}"
  [[ -x "${JAVA_HOME}/bin/java" && -x "${JAVA_HOME}/bin/javac" ]] || die 'OpenJDK 17 missing.'
  "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q 'version "17\.' || die 'Expected Java 17.'
  pass 'JDK17 available'
}

# Apache *.sha512 files may have a "filename:" prefix and wrap the 128 hex chars.
verify_archive() {
  local expected actual
  expected="$(sed '1s/^[^:]*://' "${DOWNLOADS}/${ARCHIVE}.sha512" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
  actual="$(sha512sum "${DOWNLOADS}/${ARCHIVE}" | awk '{print $1}')"
  [[ "${#expected}" -eq 128 && "$expected" =~ ^[0-9a-f]{128}$ ]] || die 'Unexpected Apache SHA-512 file format.'
  [[ "$expected" == "$PINNED_SHA512" ]] || die 'APACHE_SHA512_CHANGED: unexpected digest published for pinned version. Do not install.'
  [[ "$expected" == "$actual" ]] || die 'SHA512_MISMATCH: keep files for inspection; do not install.'
  pass 'SHA512_PASS'
}

install_kafka() {
  if [[ -d "$TARGET" ]]; then
    [[ -x "${TARGET}/bin/kafka-server-start.sh" && -x "${TARGET}/bin/kafka-storage.sh" ]] || die "Incomplete existing installation at $TARGET; refusing overwrite."
    info "Reusing ${TARGET}"
  else
    mkdir -p "$DOWNLOADS"
    local name part
    for name in "${ARCHIVE}" "${ARCHIVE}.sha512"; do
      if [[ ! -s "${DOWNLOADS}/${name}" ]]; then
        part="${DOWNLOADS}/${name}.part.$$"
        info "Downloading ${name} from Apache"
        if ! curl --fail --location --retry 3 --connect-timeout 20 --output "$part" "${RELEASE_URL}/${name}"; then
          rm -f -- "$part"
          die "Failed to download ${name}"
        fi
        mv -- "$part" "${DOWNLOADS}/${name}"
      fi
    done
    verify_archive
    tar -tzf "${DOWNLOADS}/${ARCHIVE}" >/dev/null || die 'Corrupt Kafka .tgz archive.'
    info "Extracting to /opt (sudo required)"
    sudo tar -xzf "${DOWNLOADS}/${ARCHIVE}" -C /opt/
    [[ -x "${TARGET}/bin/kafka-server-start.sh" ]] || die 'Kafka extraction failed.'
  fi
  if [[ -L "$KAFKA_HOME" ]]; then
    [[ "$(readlink -f "$KAFKA_HOME")" == "$TARGET" ]] || die '/opt/kafka points to another version; refusing replacement.'
  elif [[ -e "$KAFKA_HOME" ]]; then
    die '/opt/kafka already exists and is not the expected symlink; refusing replacement.'
  else
    sudo ln -s "$TARGET" "$KAFKA_HOME"
  fi
  [[ -x "${KAFKA_HOME}/bin/kafka-topics.sh" ]] || die 'Kafka CLI missing.'
  local version
  version="$("${KAFKA_HOME}/bin/kafka-topics.sh" --version 2>&1)" || die 'Kafka CLI version check failed.'
  [[ "$version" == "$KAFKA_VERSION"* ]] || die "Expected Kafka $KAFKA_VERSION; got $version"
  pass "Kafka ${version} installed at ${KAFKA_HOME}"
}

check_kafka_install() {
  [[ -L "$KAFKA_HOME" && "$(readlink -f "$KAFKA_HOME")" == "$TARGET" ]] || die '/opt/kafka symlink missing or unexpected.'
  [[ -x "${KAFKA_HOME}/bin/kafka-topics.sh" && -x "${KAFKA_HOME}/bin/kafka-storage.sh" ]] || die 'Kafka scripts missing.'
  local version
  version="$("${KAFKA_HOME}/bin/kafka-topics.sh" --version 2>&1)" || die 'Cannot run Kafka CLI.'
  [[ "$version" == "$KAFKA_VERSION"* ]] || die "Kafka version mismatch: $version"
  pass 'Kafka binaries verified'
}

# This exact config matches modules/kafka_ingestion/src/kafka_lifecycle.py defaults.
write_config_if_missing() {
  mkdir -p "${LOCAL_ROOT}/config" "$DATA" "$LOG_DIR"
  [[ -w "$DATA" && -w "$LOG_DIR" ]] || die 'Kafka data/log directory is not writable.'
  if [[ ! -e "$CONFIG" ]]; then
    cat > "$CONFIG" <<PROPERTIES
# Single-node KRaft Kafka $KAFKA_VERSION for local WSL2 development only.
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=${DATA}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
auto.create.topics.enable=false
log.retention.hours=168
PROPERTIES
    pass "Created $CONFIG"
  else
    info "Preserving existing config: $CONFIG"
  fi
  validate_config
}

validate_config() {
  [[ -f "$CONFIG" && -r "$CONFIG" ]] || die "Config missing: $CONFIG"
  local required
  for required in \
    'process.roles=broker,controller' \
    'node.id=1' \
    'controller.quorum.bootstrap.servers=127.0.0.1:9093' \
    'listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093' \
    'advertised.listeners=PLAINTEXT://127.0.0.1:9092' \
    'controller.listener.names=CONTROLLER' \
    "log.dirs=${DATA}"; do
    grep -Fxq -- "$required" "$CONFIG" || die "Config key missing/different: $required"
  done
  if grep -Eq '^[[:space:]]*controller\.quorum\.voters[[:space:]]*=' "$CONFIG"; then
    die 'Config mixes static and dynamic KRaft voters.'
  fi
  awk -F= '/^[[:space:]]*[a-zA-Z][a-zA-Z0-9._-]*[[:space:]]*=/ {
    gsub(/[[:space:]]/, "", $1)
    if (seen[$1]++) exit 2
  }' "$CONFIG" || die 'Duplicate config keys found.'
  pass 'Kafka configuration valid for this local single-node setup'
}

# Read Kafka Java .properties (only simple unescaped entries are used in meta.properties).
prop() { sed -n "s/^$1=//p" "$2" | head -n 1; }

check_storage() {
  [[ -f "${DATA}/meta.properties" ]] || die 'KRaft storage not formatted; run setup (without --check).'
  local id node saved
  id="$(prop 'cluster.id' "${DATA}/meta.properties")"
  node="$(prop 'node.id' "${DATA}/meta.properties")"
  [[ -n "$id" && "$node" == 1 ]] || die 'Unexpected KRaft meta.properties; refusing to proceed.'
  if [[ -s "$CLUSTER_ID_FILE" ]]; then
    saved="$(tr -d '\r\n' < "$CLUSTER_ID_FILE")"
    [[ "$saved" == "$id" ]] || die 'cluster.id file does not match formatted Kafka storage.'
  fi
  pass "KRaft storage verified (cluster.id=${id}, node.id=${node})"
}

format_storage_if_new() {
  if [[ -f "${DATA}/meta.properties" ]]; then
    # Existing installation: never format again.
    check_storage
    if [[ ! -e "$CLUSTER_ID_FILE" ]]; then
      prop 'cluster.id' "${DATA}/meta.properties" > "$CLUSTER_ID_FILE"
      pass 'Recovered cluster.id from existing meta.properties'
    fi
    return
  fi
  [[ ! -e "${DATA}/meta.properties" ]] || die 'Unexpected meta.properties file type.'
  [[ -z "$(find "$DATA" -mindepth 1 -print -quit)" ]] || die 'Storage is NOT EMPTY and has no meta.properties. Refusing to format.'
  local id
  if [[ -s "$CLUSTER_ID_FILE" ]]; then
    id="$(tr -d '\r\n' < "$CLUSTER_ID_FILE")"
    info 'Reusing saved cluster ID from incomplete prior setup.'
  elif [[ -e "$CLUSTER_ID_FILE" ]]; then
    die 'cluster.id exists but is empty; refusing to replace it.'
  else
    id="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
    [[ -n "$id" ]] || die 'Kafka failed to generate cluster ID.'
    printf '%s\n' "$id" > "$CLUSTER_ID_FILE"
    pass 'Generated cluster ID'
  fi
  info 'Formatting EMPTY KRaft storage once with --standalone'
  "${KAFKA_HOME}/bin/kafka-storage.sh" format --standalone --cluster-id "$id" --config "$CONFIG"
  check_storage
  pass 'KRaft format complete'
}

# Match Kafka Java processes by the EXACT config path, as Python lifecycle does.
matching_processes() {
  python3 - "$CONFIG" <<'PY'
import os, pathlib, sys
expected = pathlib.Path(sys.argv[1]).resolve()
for process in pathlib.Path('/proc').iterdir():
    if not process.name.isdigit():
        continue
    try:
        args = (process / 'cmdline').read_bytes().decode('utf-8', errors='replace').split('\x00')
        if 'kafka.Kafka' not in args:
            continue
        idx = args.index('kafka.Kafka')
        if idx + 1 < len(args) and pathlib.Path(args[idx+1]).resolve() == expected:
            stat = (process / 'stat').read_text().split()
            if stat[2] != 'Z':
                print(process.name)
    except (PermissionError, OSError, IndexError, ValueError):
        pass
PY
}

port_listening() {
  ss -lnt | awk 'NR>1 && $4 ~ /:(9092|9093)$/ { print $4 }'
}

broker_ready() {
  timeout 12s "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1
}

quorum_ready() {
  local result cid
  result="$(timeout 20s "${KAFKA_HOME}/bin/kafka-metadata-quorum.sh" \
    --bootstrap-server "$BOOTSTRAP" describe --status 2>&1)" || return 1
  cid="$(prop 'cluster.id' "${DATA}/meta.properties")"
  grep -Eq "^ClusterId:[[:space:]]+${cid}$" <<< "$result" || return 1
  grep -Eq '^LeaderId:[[:space:]]+1$' <<< "$result" || return 1
  grep -Eq 'CurrentVoters:.*"id":[[:space:]]*1([,}])' <<< "$result" || return 1
  return 0
}

start_and_verify() {
  local pids ports deadline startup_grace
  pids="$(matching_processes)"
  [[ "$(wc -w <<< "$pids")" -le 1 ]] || die "Multiple Kafka processes with config $CONFIG: $pids"
  if [[ -n "$pids" ]]; then
    info "Kafka already running with this config (pid=${pids}); no duplicate start."
  else
    ports="$(port_listening)"
    [[ -z "$ports" ]] || die "Port 9092 or 9093 occupied by another process: $ports"
    info 'Starting Kafka with 512MiB JVM heap (on-demand; no systemd autostart)'
    LOG_DIR="$LOG_DIR" KAFKA_HEAP_OPTS='-Xms512m -Xmx512m' \
      "${KAFKA_HOME}/bin/kafka-server-start.sh" -daemon "$CONFIG"
  fi
  deadline=$((SECONDS + 120))
  startup_grace=$((SECONDS + 20))
  while (( SECONDS < deadline )); do
    if broker_ready; then
      pass 'BROKER_READY (Kafka CLI metadata request succeeded)'
      quorum_ready || die 'Broker reachable but KRaft quorum verification failed; inspect logs.'
      pass 'KRAFT_QUORUM_READY (LeaderId=1, ClusterId matches storage)'
      return
    fi
    pids="$(matching_processes)"
    # The -daemon wrapper can return before Java appears in /proc.
    if [[ -z "$pids" ]] && (( SECONDS >= startup_grace )); then
      die "No Kafka process detected after startup grace period. Check ${LOG_DIR}/kafkaServer.out"
    fi
    sleep 3
  done
  die "Kafka not ready after 120s. Check ${LOG_DIR}/kafkaServer.out; do not blindly restart."
}

require_platform
if [[ "$MODE" == --check ]]; then
  check_prerequisites
  check_kafka_install
  validate_config
  check_storage
  broker_ready || die 'Broker not reachable at 127.0.0.1:9092; start Kafka first.'
  pass 'BROKER_READY'
  quorum_ready || die 'KRaft quorum not ready or wrong cluster.'
  pass 'KRAFT_QUORUM_READY'
  pass 'SETUP_KAFKA_PASS (read-only check)'
  exit 0
fi

info 'Beginning idempotent Kafka bootstrap (M02-01..M02-06).'
info 'Kafka data is preserved across runs. This script NEVER reformats nonempty storage.'
setup_prerequisites
install_kafka
write_config_if_missing
format_storage_if_new
start_and_verify
pass 'SETUP_KAFKA_PASS'
info 'Next: make -C modules/kafka_ingestion topics && make -C modules/kafka_ingestion topics-check'
info 'To stop later: make -C modules/kafka_ingestion kafka-stop (if Python module is installed).'
