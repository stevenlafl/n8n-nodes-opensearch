import type { Config } from 'jest';

const config: Config = {
	preset: 'ts-jest',
	testEnvironment: 'node',
	roots: ['<rootDir>/nodes', '<rootDir>/credentials', '<rootDir>/utils', '<rootDir>/tests'],
	testMatch: ['**/*.test.ts'],
	moduleFileExtensions: ['ts', 'js', 'json'],
	collectCoverageFrom: [
		'nodes/**/*.ts',
		'credentials/**/*.ts',
		'utils/**/*.ts',
		'!**/*.test.ts',
		'!**/*.d.ts',
		'!**/node_modules/**',
	],
	coverageDirectory: 'coverage',
	coverageReporters: ['text', 'lcov', 'html'],
	transform: {
		'^.+\\.ts$': ['ts-jest', {
			tsconfig: 'tsconfig.json',
		}],
	},
	moduleNameMapper: {
		'^@/(.*)$': '<rootDir>/$1',
	},
	globalSetup: '<rootDir>/tests/globalSetup.ts',
	setupFilesAfterEnv: ['<rootDir>/tests/setup.ts'],
	testTimeout: 30000,
	verbose: true,
};

export default config;
