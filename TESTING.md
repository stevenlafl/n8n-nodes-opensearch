# Testing Guide

This document describes how to run all tests for `n8n-nodes-opensearch` against different version combinations of OpenSearch and n8n.

## Prerequisites

- Node.js >= 18.10
- pnpm >= 9.1
- Docker and Docker Compose

## Quick Start

```bash
# Install dependencies
pnpm install

# Run unit tests (no Docker required)
pnpm test:unit

# Run all tests including integration (requires Docker)
docker compose -f docker-compose.test.yml --profile v3 up -d
pnpm test

# Install the packed package into fresh n8n containers and run a workflow through each
pnpm build && pnpm pack --pack-destination /tmp
scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-*.tgz
```

## Test Types

### Unit Tests

Unit tests run without any external dependencies and test node descriptions, configuration, and mock API interactions.

```bash
# Run all unit tests
pnpm test:unit

# Run with coverage
pnpm test:coverage

# Run in watch mode
pnpm test:watch
```

Test files are colocated with source code:
- `nodes/OpenSearch/GenericFunctions.test.ts`
- `nodes/OpenSearch/OpenSearch.node.test.ts`
- `nodes/vector_store/VectorStoreOpenSearch/VectorStoreOpenSearch.node.test.ts`
- `nodes/vector_store/shared/createVectorStoreNode/operations/__tests__/*.test.ts`

`tests/runtimeImports.test.ts` checks every source file only value-imports `@langchain/core`, `n8n-workflow`, `zod`, `lodash` or `@opensearch-project/opensearch`. n8n installs community packages with dev and peer dependencies stripped and resolves the rest through its own `node_modules`, and since n8n 2.29 that exposes no other langchain package. Type-only imports are fine, they are erased at build time.

### Integration Tests

Integration tests require a running OpenSearch instance and test actual API interactions.

```bash
# Start OpenSearch 3.x
docker compose -f docker-compose.test.yml --profile v3 up -d

# Wait for OpenSearch to be healthy
curl -sk https://localhost:9200 -u "admin:MyStr0ng#Pass!2024"

# Run integration tests
pnpm test:integration
```

Test files:
- `tests/integration/OpenSearch.integration.test.ts`
- `tests/integration/VectorStore.integration.test.ts`

### n8n Install Tests

`scripts/test-n8n-versions.sh` is the closest thing to a user installing the package from the community nodes UI. For each n8n version it:

1. brings up the `v3` and `install-test` profiles of `docker-compose.test.yml` with a fresh OpenSearch, a stub OpenAI API (`scripts/n8n-integration/fake-openai.py`: deterministic word-hash embeddings and canned chat completions that call the first tool offered, so no API key or model is needed) and an n8n container of that version with nothing from the repo mounted
2. installs the packed tarball into `~/.n8n/nodes` exactly like n8n's `CommunityPackagesService` does: dev, peer and optional dependencies stripped, then `npm install`
3. imports `scripts/n8n-integration/credentials.json` and `scripts/n8n-integration/workflow.json`, which covers every operation of both nodes: index create/get/get all/delete, document create/get/update/get all/search/delete, vector store insert/load/update, the vector store as a retriever behind a Question and Answer chain, the vector store as an agent tool, and the OpenSearch node as an agent tool with `$fromAI`
4. runs the workflow with `n8n execute`, reads the execution back from n8n's database with `scripts/n8n-integration/dump-execution.js` and checks every node's output with `scripts/n8n-integration/assert.js` (right document returned for "cats" and "owls", tools actually invoked, tool results visible in the agent output)
5. tears everything down, volumes included, and moves on to the next version

```bash
pnpm build && pnpm pack --pack-destination /tmp

# All target versions, one after another
scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-*.tgz

# One version
scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-*.tgz 2.38.7

# Leave the containers up after a failure
KEEP=1 scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-*.tgz 2.38.7
```

The target versions live in `TARGET_VERSIONS` at the top of the script (`--list` prints them):

| n8n | Why |
|-----|-----|
| 1.120.4 | Oldest reported install (issue #6), ships langchain 0.3 and no `@langchain/classic` |
| 1.123.7 | Same as the `n8n-1x` compose service |
| 2.1.0 | Same as the `n8n-2x` compose service |
| 2.38.7 | pnpm layout since 2.29 where only `@langchain/core` is visible to community packages (issue #8) |

The script shares container names with the manual environment below, so stop that first.

## OpenSearch Versions

The project supports both OpenSearch 2.x and 3.x. The key difference is k-NN engine support:

| Engine | OpenSearch 2.x | OpenSearch 3.x |
|--------|----------------|----------------|
| lucene | Supported | Supported (recommended) |
| faiss  | Supported | Supported |
| nmslib | Supported | **Deprecated** (fails) |

### Testing with OpenSearch 3.x

```bash
# Start OpenSearch 3.0.0
docker compose -f docker-compose.test.yml --profile v3 up -d

# Verify version
curl -sk https://localhost:9200 -u "admin:MyStr0ng#Pass!2024" | jq '.version.number'

# Check available k-NN engines
curl -sk https://localhost:9200/_plugins/_knn/stats -u "admin:MyStr0ng#Pass!2024" | jq '.nodes[].lucene_initialized, .nodes[].faiss_initialized, .nodes[].nmslib_initialized'

# Run tests
pnpm test:integration

# Stop
docker compose -f docker-compose.test.yml --profile v3 down
```

### Testing with OpenSearch 2.x

```bash
# Start OpenSearch 2.19.0 (port 9201)
docker compose -f docker-compose.test.yml --profile v2 up -d

# Verify version
curl -sk https://localhost:9201 -u "admin:MyStr0ng#Pass!2024" | jq '.version.number'

# Run tests
pnpm test:integration

# Stop
docker compose -f docker-compose.test.yml --profile v2 down
```

## n8n Versions

The project includes n8n containers for manual testing of the nodes in a real n8n environment. These mount the repo into n8n's `custom` directory, so they see the repo's own `node_modules`. That is convenient for development but hides missing dependencies, which is what the install tests above are for.

### Testing with n8n 2.x

```bash
# Build the project first
pnpm build

# Start OpenSearch 3.x + n8n 2.1.0
docker compose -f docker-compose.test.yml --profile v3 --profile n8n-2x up -d

# Access n8n at http://localhost:5678
# OpenSearch is available at https://opensearch:9200 from within n8n

# Stop
docker compose -f docker-compose.test.yml --profile v3 --profile n8n-2x down
```

### Testing with n8n 1.x

```bash
# Build the project first
pnpm build

# Start OpenSearch 3.x + n8n 1.123.7
docker compose -f docker-compose.test.yml --profile v3 --profile n8n-1x up -d

# Access n8n at http://localhost:5679

# Stop
docker compose -f docker-compose.test.yml --profile v3 --profile n8n-1x down
```

### Running Both n8n Versions Simultaneously

You can run both n8n 1.x and 2.x at the same time for comparison testing:

```bash
pnpm build

# Start OpenSearch 3.x + both n8n versions
docker compose -f docker-compose.test.yml --profile v3 --profile n8n-1x --profile n8n-2x up -d

# n8n 2.x: http://localhost:5678
# n8n 1.x: http://localhost:5679

# Stop
docker compose -f docker-compose.test.yml --profile v3 --profile n8n-1x --profile n8n-2x down
```

### Running Both OpenSearch Versions Simultaneously

You can run both OpenSearch 2.x and 3.x at the same time:

```bash
# Start both OpenSearch versions + n8n
docker compose -f docker-compose.test.yml --profile os-both --profile n8n-2x up -d

# OpenSearch 3.x: https://localhost:9200
# OpenSearch 2.x: https://localhost:9201
# n8n 2.x: http://localhost:5678
```

### n8n Credentials Configuration

Both OpenSearch versions have security enabled with HTTPS and the same credentials:

| Field | OpenSearch 3.x | OpenSearch 2.x |
|-------|----------------|----------------|
| Base URL (from n8n) | `https://opensearch:9200` | `https://opensearch-2x:9200` |
| Base URL (from host) | `https://localhost:9200` | `https://localhost:9201` |
| Username | `admin` | `admin` |
| Password | `MyStr0ng#Pass!2024` | `MyStr0ng#Pass!2024` |
| Ignore SSL Issues | Yes | Yes |

**Note:** Use `opensearch` or `opensearch-2x` as the hostname (not `localhost`) because n8n runs inside Docker and connects via the internal network.

## Version Compatibility Matrix

Run tests against all combinations to ensure compatibility:

| OpenSearch | n8n | Port | Command |
|------------|-----|------|---------|
| 3.0.0 | 2.1.0 | 5678 | `docker compose -f docker-compose.test.yml --profile v3 --profile n8n-2x up -d` |
| 3.0.0 | 1.123.7 | 5679 | `docker compose -f docker-compose.test.yml --profile v3 --profile n8n-1x up -d` |
| 2.19.0 | 2.1.0 | 5678 | `docker compose -f docker-compose.test.yml --profile v2 --profile n8n-2x up -d` |
| 2.19.0 | 1.123.7 | 5679 | `docker compose -f docker-compose.test.yml --profile v2 --profile n8n-1x up -d` |

## Full Test Suite

To run the complete test suite against all OpenSearch versions:

```bash
#!/bin/bash
set -e

echo "=== Running Unit Tests ==="
pnpm test:unit

echo "=== Testing OpenSearch 3.0.0 ==="
docker compose -f docker-compose.test.yml --profile v3 up -d --wait
pnpm test:integration
docker compose -f docker-compose.test.yml --profile v3 down

echo "=== Testing OpenSearch 2.19.0 ==="
docker compose -f docker-compose.test.yml --profile v2 up -d --wait
pnpm test:integration
docker compose -f docker-compose.test.yml --profile v2 down

echo "=== Testing fresh n8n installs ==="
pnpm build && pnpm pack --pack-destination /tmp
scripts/test-n8n-versions.sh /tmp/n8n-nodes-opensearch-*.tgz

echo "=== All Tests Passed ==="
```

## Docker Compose Profiles

| Profile | Service | Description | Port |
|---------|---------|-------------|------|
| `v3` | opensearch | OpenSearch 3.0.0 (HTTPS) | 9200 |
| `v2` | opensearch-2x | OpenSearch 2.19.0 (HTTPS) | 9201 |
| `os-both` | opensearch + opensearch-2x | Both OpenSearch versions | 9200, 9201 |
| `n8n-2x` | n8n-2x | n8n 2.1.0 with the repo mounted | 5678 |
| `n8n-1x` | n8n-1x | n8n 1.123.7 with the repo mounted | 5679 |
| `n8n-both` | n8n-1x + n8n-2x | Both n8n versions | 5678, 5679 |
| `install-test` | embeddings + n8n-install | Stub embeddings API and a fresh n8n of `$N8N_VERSION` for `scripts/test-n8n-versions.sh` | none |
| `dashboards` | opensearch-dashboards | OpenSearch Dashboards 3.0.0 | 5601 |

## Testing AI Tool Functionality

The OpenSearch node can be used as an AI Agent tool (`usableAsTool: true`). Testing this requires manual verification in n8n.

### Setup

1. Build and start n8n:
   ```bash
   pnpm build
   docker compose -f docker-compose.test.yml --profile v3 --profile n8n-2x up -d
   ```

2. In n8n, create a workflow with:
   - Chat Trigger
   - AI Agent node
   - OpenSearch Tool node (connected to Agent's tools input)

3. Configure the OpenSearch tool:
   - Set credentials for OpenSearch
   - Set Resource: Document, Operation: Search Index
   - Set Index ID
   - For Query field, click "let the model define the parameters" or set:
     ```
     {{ $fromAI('Query', 'Search term to find documents', 'string') }}
     ```

### Key Testing Points

**Query Parameter Type:**
- The `$fromAI()` must use `'string'` type (not `'json'`)
- If you see Zod validation errors like "Value must be a non-empty object or a non-empty array", the $fromAI is using `'json'` type - change it to `'string'`

**Query Formats:**
- Plain text: `"my search"` → converted to `query_string` search
- JSON: `{"query": {"match_all": {}}}` → passed directly to OpenSearch

**n8n Version Differences:**
- n8n 1.x (AgentV2): Validates tool inputs with Zod schemas directly via LangChain's `DynamicStructuredTool.call()`
- n8n 2.x (AgentV3): Uses `EngineRequest` pattern which handles inputs differently

### Verifying AI Tool Input

To see what the AI sends to the tool:
1. Execute the workflow
2. Click on the OpenSearch node in the execution
3. Check the Input panel to see the exact values passed

If you see `400 Bad Request` errors, the AI likely sent an invalid query format. Check the error message for details.

## Troubleshooting

### OpenSearch won't start

Check if another service is using port 9200:
```bash
lsof -i :9200
```

Check container logs:
```bash
docker logs opensearch-test
```

### n8n can't find the custom node

Ensure you've built the project:
```bash
pnpm build
```

Check n8n logs for loading errors:
```bash
docker logs n8n-test-2x 2>&1 | grep -i "opensearch\|error"
```

### Package loads in the mounted n8n but not from the community nodes UI

The mounted containers see the repo's `node_modules`. A real install only gets `dependencies` plus what n8n's own `node_modules` exposes. Run `pnpm test:unit` (`tests/runtimeImports.test.ts` names the offending import) and `scripts/test-n8n-versions.sh`.

### Integration tests fail with connection errors

Wait for OpenSearch to be fully healthy:
```bash
# Check health
curl -sk https://localhost:9200/_cluster/health -u "admin:MyStr0ng#Pass!2024" | jq '.status'

# Should return "green" or "yellow"
```

### NMSLIB test behavior

The NMSLIB integration test is designed to handle both OpenSearch versions:
- On 2.x: Test passes (nmslib works, logs a note)
- On 3.x: Test passes (nmslib fails as expected, error is verified)

## Coverage Report

Generate a coverage report:
```bash
pnpm test:coverage
```

Coverage report is generated in `coverage/` directory. Open `coverage/lcov-report/index.html` in a browser to view.

## CI/CD

`.github/workflows/ci.yml` runs on every push to master and every pull request:

- `unit-tests`: lint, build, `pnpm test:unit`, then packs the tarball as an artifact
- `integration-tests`: `pnpm test:integration` against an OpenSearch 3.0.0 service container
- `n8n-install-tests`: one matrix job per version from `scripts/test-n8n-versions.sh --list`, each installing that tarball into a fresh n8n and executing the integration workflow

`.github/workflows/publish.yml` runs when a push to master changes `package.json`. If the version is not on npm yet it lints, builds, runs the unit tests, publishes with provenance, tags the commit `v<version>` and mirrors the package to GitHub Packages. npm auth comes from a Trusted Publisher entry for this repository on npmjs.com, or an `NPM_TOKEN` repository secret.
