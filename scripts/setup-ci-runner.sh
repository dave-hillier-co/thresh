#!/usr/bin/env bash
#
# Install a GitHub Actions self-hosted runner for this repo on the Mac mini.
#
# Run it from a machine that can reach the mini over SSH and has `gh` logged in
# with admin rights on the repo (the registration token is minted here and piped
# over, so the mini needs neither `gh` nor a GitHub login):
#
#   ./scripts/setup-ci-runner.sh
#
# Or run it directly ON the mini, having minted a token yourself:
#
#   RUNNER_TOKEN=xxxx LOCAL=1 ./scripts/setup-ci-runner.sh
#
# Idempotent. Re-running reconfigures the existing runner in place (`--replace`)
# rather than stacking up duplicates, so it is the right way to recover a runner
# that has gone offline or had its token expire.
#
# The mini already hosts a runner for another repo. Runners register to exactly
# one repo, so this installs a SECOND, independent one in its own directory with
# its own launchd service. It does not touch the existing one.

set -euo pipefail

REPO="${REPO:-dave-hillier-co/thresh}"
REMOTE_HOST="${REMOTE_HOST:-daves-mac-mini.local}"
REMOTE_USER="${REMOTE_USER:-davehillier}"
RUNNER_VERSION="${RUNNER_VERSION:-2.336.0}"
RUNNER_DIR="${RUNNER_DIR:-\$HOME/actions-runner-thresh}"
RUNNER_NAME="${RUNNER_NAME:-mac-mini-thresh}"
RUNNER_LABELS="${RUNNER_LABELS:-self-hosted,macOS,ARM64,thresh}"

# The body that actually runs on the mini. Kept in one place so the local and
# over-SSH paths cannot drift apart.
remote_script() {
  cat <<REMOTE
set -euo pipefail
export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

RUNNER_DIR="${RUNNER_DIR}"
VERSION="${RUNNER_VERSION}"
TARBALL="actions-runner-osx-arm64-\${VERSION}.tar.gz"

if [ "\$(uname -m)" != "arm64" ]; then
  echo "expected an Apple-silicon mac; got \$(uname -m). Set RUNNER_VERSION/arch by hand." >&2
  exit 1
fi

mkdir -p "\$RUNNER_DIR"
cd "\$RUNNER_DIR"

# Stop an existing service before reconfiguring, or config.sh refuses.
# No sudo: the macOS runner installs as a per-user launchd agent, and a sudo
# password prompt over a non-interactive SSH session would hang forever.
if [ -f ./svc.sh ]; then
  echo "-- existing runner found, stopping its service"
  ./svc.sh stop >/dev/null 2>&1 || true
fi

if [ ! -f ./config.sh ]; then
  echo "-- downloading runner \$VERSION"
  curl -fsSL -o "\$TARBALL" \
    "https://github.com/actions/runner/releases/download/v\${VERSION}/\${TARBALL}"
  tar xzf "\$TARBALL"
  rm -f "\$TARBALL"
else
  echo "-- runner already downloaded"
fi

echo "-- configuring for ${REPO} as ${RUNNER_NAME}"
./config.sh \
  --url "https://github.com/${REPO}" \
  --token "\$RUNNER_TOKEN" \
  --name "${RUNNER_NAME}" \
  --labels "${RUNNER_LABELS}" \
  --work _work \
  --unattended \
  --replace

echo "-- installing and starting the launchd service"
./svc.sh install
./svc.sh start
./svc.sh status || true

echo "-- toolchain check (the workflow needs these on PATH)"
for t in node pnpm docker git; do
  if command -v "\$t" >/dev/null 2>&1; then
    echo "   ok   \$t -> \$(command -v \$t)"
  else
    echo "   MISSING \$t -- install it on the mini or CI will fail at preflight"
  fi
done
REMOTE
}

if [ "${LOCAL:-0}" = "1" ]; then
  : "${RUNNER_TOKEN:?set RUNNER_TOKEN when running with LOCAL=1}"
  export RUNNER_TOKEN
  remote_script | bash
  exit 0
fi

command -v gh >/dev/null || { echo "gh is required to mint the registration token" >&2; exit 1; }

echo "=== Minting a registration token for ${REPO} ==="
# Short-lived (about an hour) and single-use, which is why it is minted per run
# rather than stored anywhere.
TOKEN="$(gh api -X POST "repos/${REPO}/actions/runners/registration-token" --jq '.token')"
[ -n "$TOKEN" ] || { echo "failed to mint a registration token" >&2; exit 1; }

echo "=== Checking ${REMOTE_HOST} is reachable ==="
if ! ssh -o ConnectTimeout=10 -o BatchMode=yes "${REMOTE_USER}@${REMOTE_HOST}" true 2>/dev/null; then
  echo "cannot reach ${REMOTE_USER}@${REMOTE_HOST} over SSH." >&2
  echo "Check the mini is awake and on the same network, or set REMOTE_HOST to its IP." >&2
  exit 1
fi

echo "=== Installing the runner on ${REMOTE_HOST} ==="
remote_script | ssh "${REMOTE_USER}@${REMOTE_HOST}" "RUNNER_TOKEN='${TOKEN}' bash -s"

echo ""
echo "=== Done ==="
echo "Verify at: https://github.com/${REPO}/settings/actions/runners"
