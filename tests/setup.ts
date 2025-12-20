/* eslint-disable @n8n/community-nodes/no-restricted-globals, no-console */
// Jest setup file
// This runs before each test file

// Increase timeout for integration tests
jest.setTimeout(30000);

// Suppress console logs during tests unless debugging
if (process.env.DEBUG !== 'true') {
	global.console = {
		...console,
		log: jest.fn(),
		debug: jest.fn(),
		info: jest.fn(),
		// Keep warn and error for test debugging
		warn: console.warn,
		error: console.error,
	};
}
