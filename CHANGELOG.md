# Changelog

All notable changes to this project will be documented in this file.

## [0.2.6] - 2026-09-14

### Fixed
- Package failed to load when installed through the community nodes UI with
  `Cannot find module '@langchain/classic/agents'` on n8n 1.120 (#6) and on n8n 2.29 and later (#8).
  n8n installs a community package with its dev and peer dependencies stripped and resolves
  everything else through its own `node_modules`, which never contained `@langchain/classic` on
  1.120 and, since the pnpm layout in 2.29, exposes only `@langchain/core` of the langchain packages.
  Runtime imports are now limited to `@langchain/core`, `n8n-workflow`, `zod`, `lodash` and the
  bundled `@opensearch-project/opensearch`. The bind mount used for manual testing had hidden this
  because it made the repo's own `node_modules` visible to n8n.

### Added
- `tests/runtimeImports.test.ts` fails on any new runtime import outside that allowlist
- `scripts/test-n8n-versions.sh` installs the packed tarball into fresh n8n 1.120.4, 1.123.7, 2.1.0
  and 2.38.7 containers the way the community nodes UI does and executes a workflow covering every
  operation of both nodes, including the vector store as retriever and as agent tool and the
  OpenSearch node as agent tool, against a stub OpenAI API; CI runs it for every version on each
  pull request (`install-test` compose profile)
- `publish.yml` publishes to npm with provenance when the version in `package.json` changes on
  master, tags the commit and mirrors the package to GitHub Packages

### Removed
- `N8nBinaryLoader`, `N8nJsonLoader` and `N8nTool` copies of n8n internals. The document loader
  connected to the vector store is n8n's own instance and is used through its
  `processAll`/`processItem` interface
- Unused `getConnectedTools` helper
- `@langchain/community` from `peerDependencies`

## [0.2.5] - 2025-12-19

### Added
- Dark icon variants (`opensearch.dark.svg`) for theme support
- ESLint flat config format (`eslint.config.mjs`)
- Separate `.node.json` codex files for node metadata
- Lint step in CI workflow

### Changed
- Switched to `@n8n/node-cli` for build/dev/lint/release scripts
- Updated Node.js version in CI from 20 to 22
- Renamed CI workflow from `test.yml` to `ci.yml`
- VectorStoreOpenSearch version array: `[1, 1.1, 1.2, 1.3, 2]`
  - Added integer version `2` as latest for PostgreSQL/MySQL compatibility
  - Keeps decimal versions for backward compatibility with existing workflows
- Moved codex metadata from inline TypeScript to separate `.node.json` files
- Updated documentation URLs to point to npm package
- Improved operation action names (removed articles like "a document")
- Improved operation descriptions for clarity
- Delete operations now return `{ deleted: true }` instead of API response
- Credential placeholder now uses "e.g." prefix per UX guidelines
- Boolean descriptions now start with "Whether..."

### Removed
- Inline `codex` property from node TypeScript files
- Unused `docsUrl`, `categories`, `subcategories`, `alias` from `NodeMeta` interface
- Old ESLint config files (`.eslintrc.js`, `.eslintrc.prepublish.js`)
- `gulpfile.js` (static file copying now handled by n8n-node CLI)

### Fixed
- PostgreSQL/MySQL compatibility: VectorStoreOpenSearch now uses integer version (2) as latest
  - Previously failed with "invalid input syntax for type integer: 1.3"
  - n8n stores `version.slice(-1)[0]` in INTEGER column for community packages
  - SQLite was unaffected due to dynamic typing

## [0.2.4] - 2025-12-18

### Fixed
- Module resolution issue preventing installation via n8n community package installer
- Replaced `@langchain/community` OpenSearch vectorstore with custom implementation to avoid dependency conflicts

## [0.2.3] - 2025-12-18

### Changed
- Moved `@langchain/community` and `@langchain/core` to peerDependencies
- Added sample workflows for testing

## [0.2.2] - 2025-12-18

### Fixed
- OpenSearch node not appearing in n8n 2.x search (removed incorrect 'AI' category)

### Added
- Jest test infrastructure
- nmslib tests for OpenSearch 2.x and 3.x compatibility

## [0.2.1] - 2025-12-17

### Fixed
- Missing `codex.alias` causing n8n 2.x crash on startup

### Added
- Extensive refactor and testing infrastructure

## [0.2.0] - 2025-12-17

### Added
- OpenSearch Vector Store node for AI/LangChain workflows
- Support for vector store operations: insert, load, retrieve, update, retrieve-as-tool

### Fixed
- Broken packages, switched to `@langchain/classic` for n8n-langchain compatibility
- Locked packages for new langchain version

## [0.1.3] - 2025-08-22

### Added
- Search Index operation with Scroll API for large dataset pagination (PR #3 by @inigolorente)
- Configurable scroll time with automatic cleanup

### Fixed
- HTTP methods for search operations

## [0.1.2] - 2025-07-11

### Fixed
- Package.json configuration (PR #1 by @zubial)

## [0.1.1] - 2025-07-10

### Added
- Initial release
- OpenSearch node with document and index operations
- Basic CRUD operations for documents
- Index management operations
