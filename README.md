# n8n-nodes-opensearch

This is an n8n community node. It lets you use OpenSearch in your n8n workflows.

The [OpenSearch](https://opensearch.org/) project, created by Amazon, is a forked search project based on old versions of Elasticsearch and Kibana.

[n8n](https://n8n.io/) is a [fair-code licensed](https://docs.n8n.io/reference/license/) workflow automation platform.

[Installation](#installation)
[Operations](#operations)
[Credentials](#credentials)
[Compatibility](#compatibility)
[Sample Workflows](#sample-workflows)
[Version History](#version-history)
[Resources](#resources)  

## Installation

Follow the [installation guide](https://docs.n8n.io/integrations/community-nodes/installation/) in the n8n community nodes documentation.

## Operations

**Document Actions**

 - Create a document
 - Delete a document
 - Get a document
 - Get many documents
 - Update a document

**Index Actions**

- Create an index
- Delete an index
- Get an index
- Get many indices

**OpenSearch Vector Store**

- Retrieve
- Insert
- Get Many

## Credentials

You will need a baseURL and a username and password to authenticate to the OpenSearch service.

## Compatibility

| Package Version | n8n Version | Notes |
|-----------------|-------------|-------|
| @0.1.4 | 1.48.0+ | Tested with 1.62.1 |
| @0.2.x | 2.0.0+ | Full support including AI tools |
| @0.2.x | 1.100.0+ | AI tools supported |

### Using as AI Tool

The OpenSearch node supports `usableAsTool: true`, which allows it to be used as an AI Agent tool. This creates an `opensearchTool` node type automatically.

**Requirements:**
- n8n **1.100.0 or later** is required for AI tool support with community packages. Earlier versions of n8n 1.x (< 1.100.0) only support tool nodes from built-in `n8n-nodes-base` packages.

**Query Parameter:**
The Search Index operation's Query parameter accepts:
- **Plain text**: e.g., `"my search term"` - automatically converted to a `query_string` search across all fields
- **JSON**: e.g., `{"query": {"match": {"title": "hello"}}}` - passed directly to OpenSearch

When using as an AI tool, set up the Query field with `$fromAI()`:
```
{{ $fromAI('Query', 'Search term to find documents', 'string') }}
```

**Important:** The `$fromAI()` must use `'string'` type (not `'json'`). If upgrading from an older version where you had `'json'` type, you'll need to either:
1. Manually change `'json'` to `'string'` in the expression
2. Delete and re-add the tool node, then click "let the model define the parameters"

**n8n 1.x vs 2.x:** Both versions work with AI tools. n8n 1.x uses AgentV2 which validates tool inputs with Zod schemas directly. n8n 2.x uses AgentV3 with a different execution model. Using `'string'` type ensures compatibility with both.

## Sample Workflows

Sample n8n workflows demonstrating OpenSearch node usage are available in the [`tests/workflows/`](tests/workflows/) folder:

- `opensearch-all-modes-test-1.123.7.json` - For n8n 1.x
- `opensearch-all-modes-test-2.1.0.json` - For n8n 2.x

Import these into your n8n instance to see examples of all operations including document CRUD, index management, and vector store usage.

## Version History

### 0.2.5 (2025-12-19)
- Fixed PostgreSQL/MySQL compatibility for VectorStoreOpenSearch node versioning
- Migrated to `@n8n/node-cli` for build tooling
- Added dark icon variants for theme support
- Improved UX: cleaner operation names, better descriptions, proper delete output format
- Moved codex metadata to separate `.node.json` files (scaffold pattern)

### 0.2.4 (2025-12-18)
- Fixed module resolution issue preventing installation via n8n community package installer
- Replaced `@langchain/community` OpenSearch vectorstore with custom implementation to avoid dependency conflicts

### 0.2.3 (2025-12-18)
- Moved `@langchain/community` and `@langchain/core` to peerDependencies to avoid version conflicts

### 0.2.2 (2025-12-18)
- Fixed OpenSearch node not appearing in n8n 2.x search (removed incorrect 'AI' category)

### 0.2.1 (2025-12-18)
- Extensive refactor with full test coverage
- Added support for OpenSearch 2.x and 3.x
- Added nmslib engine deprecation handling for OpenSearch 3.x
- Fixed build and packaging issues

### 0.2.0 (2025-12-17)
- Upgraded to `@langchain/classic` 1.0.5 for n8n 2.x compatibility
- Added n8n 2.x support while maintaining n8n 1.x compatibility

### 0.1.3 (2025-10-02)
- Added Search Index operation with Scroll API support
- Fixed HTTP methods for complex queries

### 0.1.2 (2025-07-10)
- Fixed missing dependency issue

### 0.1.1 (2024-10-04)
- Documentation updates

### 0.1.0 (2024-10-04)
- Initial release with OpenSearch document and index operations
- OpenSearch Vector Store for AI/LangChain workflows

## Resources

* [n8n community nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
* [OpenSearch Langchain documentation](https://js.langchain.com/docs/integrations/vectorstores/opensearch/)
