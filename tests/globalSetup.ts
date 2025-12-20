/* eslint-disable @n8n/community-nodes/no-restricted-imports, @n8n/community-nodes/no-restricted-globals, no-console */
// Jest global setup - runs BEFORE any test files are loaded
// This checks OpenSearch availability and sets environment variables

import { Client } from '@opensearch-project/opensearch';
import { OPENSEARCH_CONFIG } from './integration/config';

export default async function globalSetup() {
	const available: string[] = [];

	for (const [version, config] of Object.entries(OPENSEARCH_CONFIG.instances)) {
		try {
			const client = new Client({
				node: config.url,
				auth: { username: OPENSEARCH_CONFIG.username, password: OPENSEARCH_CONFIG.password },
				ssl: { rejectUnauthorized: false },
			});
			await client.cluster.health({ timeout: '3s' });
			await client.close();
			process.env[config.envVar] = 'true';
			available.push(version);
		} catch {
			process.env[config.envVar] = 'false';
		}
	}

	console.log(`\nOpenSearch available: ${available.length > 0 ? available.join(', ') : 'none'}\n`);
}
