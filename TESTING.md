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

### Integration Tests

Integration tests require a running OpenSearch instance and test actual API interactions.

```bash
# Start OpenSearch 3.x
docker compose -f docker-compose.test.yml --profile v3 up -d

# Wait for OpenSearch to be healthy
curl -s http://localhost:9200

# Run integration tests
pnpm test:integration
```

Test files:
- `tests/integration/OpenSearch.integration.test.ts`
- `tests/integration/VectorStore.integration.test.ts`

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
curl -s http://localhost:9200 | jq '.version.number'

# Check available k-NN engines
curl -s http://localhost:9200/_plugins/_knn/stats | jq '.nodes[].lucene_initialized, .nodes[].faiss_initialized, .nodes[].nmslib_initialized'

# Run tests
pnpm test:integration

# Stop
docker compose -f docker-compose.test.yml --profile v3 down
```

### Testing with OpenSearch 2.x

```bash
# Start OpenSearch 2.6.0
docker compose -f docker-compose.test.yml --profile v2 up -d

# Verify version
curl -s http://localhost:9200 | jq '.version.number'

# Run tests
pnpm test:integration

# Stop
docker compose -f docker-compose.test.yml --profile v2 down
```

## n8n Versions

The project includes n8n containers for manual testing of the nodes in a real n8n environment.

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
| 2.6.0 | 2.1.0 | 5678 | `docker compose -f docker-compose.test.yml --profile v2 --profile n8n-2x up -d` |
| 2.6.0 | 1.123.7 | 5679 | `docker compose -f docker-compose.test.yml --profile v2 --profile n8n-1x up -d` |

## Full Test Suite

To run the complete test suite against all OpenSearch versions:

```bash
#!/bin/bash
set -e

echo "=== Running Unit Tests ==="
pnpm test:unit

echo "=== Testing OpenSearch 3.0.0 ==="
docker compose -f docker-compose.test.yml --profile v3 up -d
sleep 10
curl -s http://localhost:9200 | jq '.version.number'
pnpm test:integration
docker compose -f docker-compose.test.yml --profile v3 down

echo "=== Testing OpenSearch 2.6.0 ==="
docker compose -f docker-compose.test.yml --profile v2 up -d
sleep 10
curl -s http://localhost:9200 | jq '.version.number'
pnpm test:integration
docker compose -f docker-compose.test.yml --profile v2 down

echo "=== All Tests Passed ==="
```

## Docker Compose Profiles

| Profile | Service | Description | Port |
|---------|---------|-------------|------|
| `v3` | opensearch | OpenSearch 3.0.0 (HTTPS) | 9200 |
| `v2` | opensearch-2x | OpenSearch 2.6.0 (HTTPS) | 9201 |
| `os-both` | opensearch + opensearch-2x | Both OpenSearch versions | 9200, 9201 |
| `n8n-2x` | n8n-2x | n8n 2.1.0 | 5678 |
| `n8n-1x` | n8n-1x | n8n 1.123.7 | 5679 |
| `n8n-both` | n8n-1x + n8n-2x | Both n8n versions | 5678, 5679 |
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

### Integration tests fail with connection errors

Wait for OpenSearch to be fully healthy:
```bash
# Check health
curl -s http://localhost:9200/_cluster/health | jq '.status'

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

For CI pipelines, use the following workflow:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        opensearch: ['2.6.0', '3.0.0']
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v2
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'pnpm'
      - run: pnpm install
      - run: pnpm test:unit
      - name: Start OpenSearch ${{ matrix.opensearch }}
        run: |
          if [ "${{ matrix.opensearch }}" = "3.0.0" ]; then
            docker compose -f docker-compose.test.yml --profile v3 up -d
          else
            docker compose -f docker-compose.test.yml --profile v2 up -d
          fi
      - name: Wait for OpenSearch
        run: |
          for i in {1..30}; do
            curl -s http://localhost:9200 && break || sleep 2
          done
      - run: pnpm test:integration
```
