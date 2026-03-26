#!/usr/bin/env bash
#
# Build the SNA agent Docker image, push to Docker Hub and Snowflake, and
# publish the Snowflake native app.
#
# Run with --help for usage information.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ── colours / helpers ────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# Returns 0 (continue), 1 (skip). Aborts the script on "abort".
prompt_gate() {
  local step_name="$1"
  if [[ "$INTERACTIVE" == "false" ]]; then
    return 0
  fi
  while true; do
    echo -en "${YELLOW}About to: ${step_name}. [c]ontinue / [s]kip / [a]bort? ${NC}"
    read -r answer </dev/tty
    case "${answer,,}" in
      c|continue) return 0 ;;
      s|skip)     info "Skipping: ${step_name}"; return 1 ;;
      a|abort)    info "Aborted."; exit 0 ;;
      *)          echo "Please enter c, s, or a." ;;
    esac
  done
}

# ── usage ────────────────────────────────────────────────────────────────────

usage() {
  cat <<'EOF'
Usage: deploy.sh [OPTIONS]

Build the SNA agent Docker image, push to Docker Hub and Snowflake image
registry, and publish the Snowflake native app.

Required arguments:
  --code-version VERSION          Image/app version tag (e.g. 1.2.3 or 1.2.3rc42)
  --docker-hub-repo REPO          Docker Hub repository name (e.g. sna-agent)
  --snowflake-account ACCOUNT     Snowflake account identifier
  --snowflake-user USER           Snowflake user
  --snowflake-role ROLE           Snowflake role
  --snowflake-warehouse WH        Snowflake warehouse
  --snowflake-private-key-path P  Path to private key .p8 file
  --snowflake-repo-url URL        Snowflake SPCS image registry URL
  --backend-url-scheme SCHEME     Backend URL protocol (e.g. https)
  --backend-url-host HOST         Backend URL host:port

Optional arguments:
  --build-number NUM              Build number for version file (default: local)
  --non-interactive               Disable interactive prompts (for CI)
  --help                          Show this help message

In interactive mode (the default), the script prompts before each publish
step with options to continue, skip, or abort.
EOF
  exit 0
}

# ── argument parsing ─────────────────────────────────────────────────────────

CODE_VERSION=""
DOCKER_HUB_REPO=""
SNOWFLAKE_ACCOUNT=""
SNOWFLAKE_USER=""
SNOWFLAKE_ROLE=""
SNOWFLAKE_WAREHOUSE=""
SNOWFLAKE_PRIVATE_KEY_PATH=""
SNOWFLAKE_REPO_URL=""
BACKEND_URL_SCHEME=""
BACKEND_URL_HOST=""
BUILD_NUMBER="local"
INTERACTIVE="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --code-version)               CODE_VERSION="$2";               shift 2 ;;
    --docker-hub-repo)            DOCKER_HUB_REPO="$2";            shift 2 ;;
    --snowflake-account)          SNOWFLAKE_ACCOUNT="$2";          shift 2 ;;
    --snowflake-user)             SNOWFLAKE_USER="$2";             shift 2 ;;
    --snowflake-role)             SNOWFLAKE_ROLE="$2";             shift 2 ;;
    --snowflake-warehouse)        SNOWFLAKE_WAREHOUSE="$2";        shift 2 ;;
    --snowflake-private-key-path) SNOWFLAKE_PRIVATE_KEY_PATH="$2"; shift 2 ;;
    --snowflake-repo-url)         SNOWFLAKE_REPO_URL="$2";         shift 2 ;;
    --backend-url-scheme)         BACKEND_URL_SCHEME="$2";         shift 2 ;;
    --backend-url-host)           BACKEND_URL_HOST="$2";           shift 2 ;;
    --build-number)               BUILD_NUMBER="$2";               shift 2 ;;
    --non-interactive)            INTERACTIVE="false";             shift ;;
    --help)                       usage ;;
    *) die "Unknown argument: $1" ;;
  esac
done

# Validate required arguments
missing=()
[[ -n "$CODE_VERSION" ]]               || missing+=("--code-version")
[[ -n "$DOCKER_HUB_REPO" ]]           || missing+=("--docker-hub-repo")
[[ -n "$SNOWFLAKE_ACCOUNT" ]]         || missing+=("--snowflake-account")
[[ -n "$SNOWFLAKE_USER" ]]            || missing+=("--snowflake-user")
[[ -n "$SNOWFLAKE_ROLE" ]]            || missing+=("--snowflake-role")
[[ -n "$SNOWFLAKE_WAREHOUSE" ]]       || missing+=("--snowflake-warehouse")
[[ -n "$SNOWFLAKE_PRIVATE_KEY_PATH" ]] || missing+=("--snowflake-private-key-path")
[[ -n "$SNOWFLAKE_REPO_URL" ]]        || missing+=("--snowflake-repo-url")
[[ -n "$BACKEND_URL_SCHEME" ]]        || missing+=("--backend-url-scheme")
[[ -n "$BACKEND_URL_HOST" ]]          || missing+=("--backend-url-host")

if [[ ${#missing[@]} -gt 0 ]]; then
  die "Missing required arguments: ${missing[*]}"
fi

[[ -f "$SNOWFLAKE_PRIVATE_KEY_PATH" ]] || die "Private key file not found: $SNOWFLAKE_PRIVATE_KEY_PATH"

# ── derived values ───────────────────────────────────────────────────────────

DOCKER_HUB_IMAGE="montecarlodata/${DOCKER_HUB_REPO}"
SNOW_CONN_FLAGS=(
  --temporary-connection
  --account "$SNOWFLAKE_ACCOUNT"
  --user "$SNOWFLAKE_USER"
  --role "$SNOWFLAKE_ROLE"
  --warehouse "$SNOWFLAKE_WAREHOUSE"
  --authenticator SNOWFLAKE_JWT
  --private-key-file "$SNOWFLAKE_PRIVATE_KEY_PATH"
)

# Cross-platform sed in-place
sed_inplace() {
  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' "$@"
  else
    sed -i "$@"
  fi
}

# ── preflight checks ────────────────────────────────────────────────────────

command -v docker >/dev/null || die "docker not found on PATH"
command -v snow   >/dev/null || die "snow (Snowflake CLI) not found on PATH"

info "=== Deploy: version=${CODE_VERSION} repo=${DOCKER_HUB_REPO} ==="

# ── 1. Replace backend URLs ─────────────────────────────────────────────────

info "Replacing backend URLs..."

sed_inplace "s|artemis.getmontecarlo.com:443|${BACKEND_URL_HOST}|g" app/scripts/setup_procs.sql
grep "host_ports" app/scripts/setup_procs.sql

sed_inplace "s|https://artemis.getmontecarlo.com:443|${BACKEND_URL_SCHEME}://${BACKEND_URL_HOST}|g" service/agent/utils/utils.py
grep -n "BACKEND_SERVICE_URL" service/agent/utils/utils.py

# ── 2. Replace image tag references ─────────────────────────────────────────

info "Replacing image tags (:latest -> :${CODE_VERSION})..."

sed_inplace "s|:latest|:${CODE_VERSION}|g" app/manifest.yml
grep -A2 "images:" app/manifest.yml

sed_inplace "s|:latest|:${CODE_VERSION}|g" service/mcd_agent_spec.yaml
grep "image:" service/mcd_agent_spec.yaml

# ── 3. Test Snowflake connection + registry login ────────────────────────────

info "Testing Snowflake connection..."
snow connection test "${SNOW_CONN_FLAGS[@]}"
success "Snowflake connection OK"

info "Logging in to Snowflake image registry..."
snow spcs image-registry login "${SNOW_CONN_FLAGS[@]}"
success "Snowflake registry login OK"

# ── 4. Build Docker image ───────────────────────────────────────────────────

info "Building Docker image..."

DOCKER_BUILDKIT=1 docker build \
  --platform linux/amd64 \
  --build-arg "code_version=${CODE_VERSION}" \
  --build-arg "build_number=${BUILD_NUMBER}" \
  -t "${DOCKER_HUB_IMAGE}:latest" \
  -t "${DOCKER_HUB_IMAGE}:${CODE_VERSION}" \
  -f service/Dockerfile \
  service/

success "Docker build complete"

# ── 5. Verify version in image ──────────────────────────────────────────────

info "Verifying version in Docker image..."

EXPECTED_VERSION="${CODE_VERSION},${BUILD_NUMBER}"
IMAGE_VERSION=$(docker run --rm --entrypoint python "${DOCKER_HUB_IMAGE}:latest" agent/utils/settings.py)

if [[ "$IMAGE_VERSION" == "$EXPECTED_VERSION" ]]; then
  success "Version verified: ${IMAGE_VERSION}"
else
  die "Version mismatch! Expected '${EXPECTED_VERSION}', got '${IMAGE_VERSION}'"
fi

# ── 6. Push to Docker Hub ───────────────────────────────────────────────────

if prompt_gate "Push ${DOCKER_HUB_IMAGE}:${CODE_VERSION} to Docker Hub"; then
  info "Pushing to Docker Hub..."
  docker push "${DOCKER_HUB_IMAGE}:latest"
  docker push "${DOCKER_HUB_IMAGE}:${CODE_VERSION}"
  success "Pushed to Docker Hub"
fi

# ── 7. Tag and push to Snowflake image registry ─────────────────────────────

if prompt_gate "Push mcd_agent:${CODE_VERSION} to Snowflake image registry"; then
  info "Pushing to Snowflake image registry..."
  docker tag "${DOCKER_HUB_IMAGE}:latest" "${SNOWFLAKE_REPO_URL}/mcd_agent:${CODE_VERSION}"
  docker push "${SNOWFLAKE_REPO_URL}/mcd_agent:${CODE_VERSION}"
  docker tag "${DOCKER_HUB_IMAGE}:latest" "${SNOWFLAKE_REPO_URL}/mcd_agent:latest"
  docker push "${SNOWFLAKE_REPO_URL}/mcd_agent:latest"
  success "Pushed to Snowflake image registry"
fi

# ── 8. Publish Snowflake native app ─────────────────────────────────────────

if prompt_gate "Publish Snowflake native app (snow app run)"; then
  info "Publishing Snowflake native app..."
  snow app run "${SNOW_CONN_FLAGS[@]}"
  success "Snowflake native app published"
fi

echo ""
success "=== Deploy complete: ${CODE_VERSION} ==="
