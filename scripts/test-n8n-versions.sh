#!/usr/bin/env bash
# Integration test of the packed package against fresh n8n installs.
#
# For each n8n version: bring up the v3 and install-test profiles of docker-compose.test.yml
# (OpenSearch, a stub embeddings API and a fresh n8n of that version), install the tarball the
# way the community nodes UI does (dev, peer and optional deps stripped, then npm install),
# import credentials and scripts/n8n-integration/workflow.json, execute the workflow with n8n's
# CLI, read the execution back from n8n's database (scripts/n8n-integration/dump-execution.js) and
# check it with scripts/n8n-integration/assert.js, tear everything down including
# volumes, then move on to the next version. Stop the manual dev environment first, the
# container names are shared.
#
#   pnpm build && pnpm pack --pack-destination /tmp
#   scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-0.2.6.tgz            # all target versions
#   scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-0.2.6.tgz 2.38.7     # just one
#   scripts/test-n8n-versions.sh --list                                          # print target versions
#
# KEEP=1 leaves the containers running after a failure for inspection.
set -euo pipefail

# Target n8n versions. 1.120.4 is the oldest reported install (issue #6, langchain 0.3 era),
# 1.123.7 and 2.1.0 match docker-compose.test.yml, 2.38.7 is the pnpm layout that hides
# everything but @langchain/core from community packages (issue #8).
TARGET_VERSIONS=(1.120.4 1.123.7 2.1.0 2.38.7)

if [ "${1:-}" = "--list" ]; then
  printf '%s\n' "${TARGET_VERSIONS[@]}"
  exit 0
fi

here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
tarball=$(readlink -f "${1:?tarball}")
shift
versions=("${@:-${TARGET_VERSIONS[@]}}")
pkg=n8n-nodes-opensearch
n8n=n8n-install-test
fail=0

compose() {
  N8N_VERSION="$version" docker compose -p "it-${version//./-}" -f "$root/docker-compose.test.yml" \
    --profile v3 --profile install-test "$@"
}

for version in "${versions[@]}"; do
  echo "== n8n $version =="
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d --wait >/dev/null

  docker cp "$tarball" "$n8n:/tmp/pkg.tgz"
  docker cp "$here/n8n-integration/credentials.json" "$n8n:/tmp/credentials.json"
  docker cp "$here/n8n-integration/workflow.json" "$n8n:/tmp/workflow.json"
  docker cp "$here/n8n-integration/dump-execution.js" "$n8n:/tmp/dump-execution.js"

  # Same steps as n8n's CommunityPackagesService.installPackage
  docker exec "$n8n" sh -ec '
    dir=/home/node/.n8n/nodes/node_modules/'"$pkg"'
    mkdir -p "$dir" && tar -xzf /tmp/pkg.tgz -C "$dir" --strip-components=1
    cd "$dir"
    node -e "
      const fs = require(\"fs\");
      const { devDependencies, peerDependencies, optionalDependencies, ...rest } = JSON.parse(fs.readFileSync(\"package.json\", \"utf8\"));
      fs.writeFileSync(\"package.json\", JSON.stringify(rest, null, 2));
    "
    npm install --audit=false --fund=false --bin-links=false --install-strategy=shallow --ignore-scripts=true --package-lock=false --loglevel=error
    printf "{\"name\":\"installed-nodes\",\"private\":true,\"dependencies\":{\"%s\":\"%s\"}}\n" '"$pkg"' "$(node -p "require(\"./package.json\").version")" > /home/node/.n8n/nodes/package.json
    n8n import:credentials --input /tmp/credentials.json >/dev/null
    n8n import:workflow --input /tmp/workflow.json >/dev/null
  '

  output=$(docker exec "$n8n" n8n execute --id opensearch-integration 2>&1 || true)
  # The run is read back from n8n's database rather than stdout so the log level does not matter
  execution=$(docker exec "$n8n" sh -c 'cd /usr/local/lib/node_modules/n8n && NODE_PATH=$(node -p "require(\"module\")._nodeModulePaths(process.cwd() + \"/dist\").join(\":\")") node /tmp/dump-execution.js' 2>&1 || true)
  if printf '%s' "$execution" | node "$here/n8n-integration/assert.js"; then
    echo "n8n $version: OK"
    compose down -v >/dev/null 2>&1
  else
    echo "n8n $version: FAIL"
    printf '%s\n' "$output" | tail -40 | sed 's/^/    /'
    fail=1
    [ -n "${KEEP:-}" ] || compose down -v >/dev/null 2>&1
  fi
done
exit $fail
