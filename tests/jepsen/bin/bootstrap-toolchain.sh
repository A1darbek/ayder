#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
JEPSEN_DIR="$ROOT_DIR/tests/jepsen"
TOOLCHAIN_DIR="${AYDER_JEPSEN_TOOLCHAIN_DIR:-$JEPSEN_DIR/.toolchain}"
JDK_DIR="$TOOLCHAIN_DIR/jdk"
BIN_DIR="$TOOLCHAIN_DIR/bin"
ENV_FILE="$TOOLCHAIN_DIR/env.sh"
LEIN_HOME_DEFAULT="$TOOLCHAIN_DIR/.lein"

IPTABLES_STAGE="$TOOLCHAIN_DIR/iptables"
IPTABLES_WRAPPER="$BIN_DIR/iptables"

detect_jdk_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      echo "x64"
      ;;
    aarch64|arm64)
      echo "aarch64"
      ;;
    *)
      echo "unsupported"
      ;;
  esac
}

bootstrap_local_iptables() {
  if command -v iptables >/dev/null 2>&1; then
    echo "[toolchain] System iptables found at $(command -v iptables)"
    return 0
  fi

  local xt_multi="$IPTABLES_STAGE/usr/sbin/xtables-nft-multi"
  if [[ ! -x "$xt_multi" ]]; then
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' RETURN

    echo "[toolchain] System iptables not found; downloading iptables userspace package..."
    (cd "$tmp_dir" && apt-get download iptables >/dev/null)

    local deb_file
    deb_file="$(find "$tmp_dir" -maxdepth 1 -name 'iptables_*_*.deb' | head -n 1 || true)"
    if [[ -z "$deb_file" ]]; then
      echo "[toolchain] Failed to download iptables package" >&2
      return 1
    fi

    rm -rf "$IPTABLES_STAGE"
    mkdir -p "$IPTABLES_STAGE"
    dpkg-deb -x "$deb_file" "$IPTABLES_STAGE"
    rm -rf "$tmp_dir"
    trap - RETURN
  else
    echo "[toolchain] Reusing local iptables userspace at $IPTABLES_STAGE"
  fi

  [[ -x "$xt_multi" ]] || {
    echo "[toolchain] xtables-nft-multi not found after bootstrap" >&2
    return 1
  }

  local xt_libdir=""
  xt_libdir="$(find "$IPTABLES_STAGE/usr/lib" -maxdepth 3 -type d -name xtables 2>/dev/null | head -n 1 || true)"

  cat > "$IPTABLES_WRAPPER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
XT_MULTI="$xt_multi"
XT_LIBDIR="$xt_libdir"
if [[ -n "\${XT_LIBDIR}" && -d "\${XT_LIBDIR}" ]]; then
  export XTABLES_LIBDIR="\${XT_LIBDIR}"
fi
exec "\${XT_MULTI}" iptables "\$@"
EOF
  chmod +x "$IPTABLES_WRAPPER"

  echo "[toolchain] Installed local iptables wrapper at $IPTABLES_WRAPPER"
  "$IPTABLES_WRAPPER" --version | head -n 1
}

JDK_ARCH="${AYDER_JEPSEN_JDK_ARCH:-$(detect_jdk_arch)}"
if [[ "$JDK_ARCH" == "unsupported" ]]; then
  echo "[toolchain] Unsupported architecture: $(uname -m)" >&2
  echo "[toolchain] Set AYDER_JEPSEN_JDK_ARCH manually (e.g., x64 or aarch64)." >&2
  exit 1
fi

JDK_URL="${AYDER_JEPSEN_JDK_URL:-https://api.adoptium.net/v3/binary/latest/21/ga/linux/$JDK_ARCH/jdk/hotspot/normal/eclipse?project=jdk}"
LEIN_URL="${AYDER_JEPSEN_LEIN_URL:-https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein}"

mkdir -p "$BIN_DIR"

need_jdk_install=1
if [[ -x "$JDK_DIR/bin/java" ]]; then
  if "$JDK_DIR/bin/java" -version >/dev/null 2>&1; then
    need_jdk_install=0
  else
    echo "[toolchain] Existing JDK is not runnable on this host, reinstalling..."
    rm -rf "$JDK_DIR"
  fi
fi

if [[ "$need_jdk_install" -eq 1 ]]; then
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT

  echo "[toolchain] Downloading JDK ($JDK_ARCH) from Adoptium..."
  curl -fsSL "$JDK_URL" -o "$tmp_dir/jdk.tar.gz"
  tar -xzf "$tmp_dir/jdk.tar.gz" -C "$tmp_dir"

  extracted_dir="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  if [[ -z "$extracted_dir" ]]; then
    echo "[toolchain] Failed to extract JDK archive" >&2
    exit 1
  fi

  rm -rf "$JDK_DIR"
  mv "$extracted_dir" "$JDK_DIR"

  rm -f "$tmp_dir/jdk.tar.gz"
  trap - EXIT
  rm -rf "$tmp_dir"
else
  echo "[toolchain] Reusing existing JDK at $JDK_DIR"
fi

if [[ ! -x "$BIN_DIR/lein" ]]; then
  echo "[toolchain] Downloading Leiningen launcher..."
  curl -fsSL "$LEIN_URL" -o "$BIN_DIR/lein"
  chmod +x "$BIN_DIR/lein"
else
  echo "[toolchain] Reusing existing Lein launcher at $BIN_DIR/lein"
fi

if ! bootstrap_local_iptables; then
  echo "[toolchain] WARNING: local iptables wrapper bootstrap failed (partition nemesis may fail)" >&2
fi

cat > "$ENV_FILE" <<EOF
export JAVA_HOME="$JDK_DIR"
export LEIN_HOME="${AYDER_JEPSEN_LEIN_HOME:-$LEIN_HOME_DEFAULT}"
export LEIN_ROOT=1
export PATH="$BIN_DIR:$JDK_DIR/bin:\$PATH"
EOF

# shellcheck disable=SC1090
source "$ENV_FILE"

mkdir -p "$LEIN_HOME"

echo "[toolchain] Bootstrapping Leiningen..."
lein -version >/dev/null

echo "[toolchain] Ready"
java -version 2>&1 | head -n 1
lein -version | head -n 1
if command -v iptables >/dev/null 2>&1; then
  echo "$(iptables --version | head -n 1)"
fi
echo "[toolchain] To use in current shell: source $ENV_FILE"