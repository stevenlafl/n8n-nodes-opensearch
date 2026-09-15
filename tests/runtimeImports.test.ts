/* eslint-disable @n8n/community-nodes/no-restricted-imports, @n8n/community-nodes/no-restricted-globals */
import { readdirSync, readFileSync, statSync } from 'fs';
import { builtinModules } from 'module';
import { join, relative } from 'path';

// n8n installs a community package with its devDependencies and peerDependencies stripped, so at
// runtime a module either comes from our own "dependencies" or from n8n's node_modules via NODE_PATH.
// Since n8n 2.29 only @langchain/core, n8n-workflow, zod and lodash are reachable that way (the other
// @langchain/* packages moved out of n8n's top-level node_modules), and n8n 1.x below 1.121 never
// shipped @langchain/classic at all. Anything else must be a type-only import or a real dependency.
const ALLOWED = ['@langchain/core', '@opensearch-project/opensearch', 'lodash', 'n8n-workflow', 'zod'];

const ROOT = join(__dirname, '..');
const SOURCE_DIRS = ['credentials', 'nodes', 'utils'];

function sourceFiles(dir: string): string[] {
	return readdirSync(dir).flatMap((name) => {
		const path = join(dir, name);
		if (statSync(path).isDirectory()) return name === '__tests__' ? [] : sourceFiles(path);
		return name.endsWith('.ts') && !name.endsWith('.test.ts') && !name.endsWith('.d.ts') ? [path] : [];
	});
}

function packageName(specifier: string): string {
	const parts = specifier.split('/');
	return specifier.startsWith('@') ? parts.slice(0, 2).join('/') : parts[0];
}

function runtimeImports(source: string): string[] {
	// Value imports and requires only. "import type" is erased by tsc and never hits require().
	const pattern = /^\s*(?:import\s+(?!type\s)[^'"]*?\sfrom\s+|import\s+|export\s+[^'"]*?\sfrom\s+|.*?require\()\s*['"]([^'"]+)['"]/gm;
	return [...source.matchAll(pattern)].map((m) => m[1]).filter((s) => !s.startsWith('.'));
}

describe('runtime imports', () => {
	const files = SOURCE_DIRS.flatMap((dir) => sourceFiles(join(ROOT, dir)));

	it('finds source files', () => {
		expect(files.length).toBeGreaterThan(0);
	});

	it.each(files.map((file) => [relative(ROOT, file), file]))(
		'%s only requires modules n8n can resolve',
		(_name, file) => {
			const external = runtimeImports(readFileSync(file, 'utf8'))
				.map(packageName)
				.filter((name) => !name.startsWith('node:') && !builtinModules.includes(name));
			expect(external.filter((name) => !ALLOWED.includes(name))).toEqual([]);
		},
	);
});
