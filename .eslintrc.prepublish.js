/**
 * @type {import('@types/eslint').ESLint.ConfigData}
 */
module.exports = {
	extends: "./.eslintrc.js",

	ignorePatterns: [
		'**/*.test.ts',
		'**/__tests__/**',
		'tests/**',
	],

	overrides: [
		{
			files: ['package.json'],
			plugins: ['eslint-plugin-n8n-nodes-base'],
			rules: {
				'n8n-nodes-base/community-package-json-name-still-default': 'error',
			},
		},
	],
};
