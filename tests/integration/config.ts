/* eslint-disable @n8n/community-nodes/no-restricted-globals */
// Shared configuration for integration tests

export const OPENSEARCH_CONFIG = {
	username: 'admin',
	password: 'MyStr0ng#Pass!2024',
	instances: {
		'3.x': { url: 'https://localhost:9200', envVar: 'OPENSEARCH_3X_AVAILABLE' },
		'2.x': { url: 'https://localhost:9201', envVar: 'OPENSEARCH_2X_AVAILABLE' },
	},
} as const;

export function isInstanceAvailable(version: '3.x' | '2.x'): boolean {
	return process.env[OPENSEARCH_CONFIG.instances[version].envVar] === 'true';
}

export function getInstanceUrl(version: '3.x' | '2.x'): string {
	return OPENSEARCH_CONFIG.instances[version].url;
}
