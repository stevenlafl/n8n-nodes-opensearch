/**
 * Integration tests for OpenSearch node operations.
 *
 * These tests verify the OpenSearch node behavior by calling node.execute()
 * with a mocked IExecuteFunctions context, but allowing real HTTP calls
 * to a running OpenSearch instance.
 *
 * Prerequisites:
 * 1. Start OpenSearch using: docker compose -f docker-compose.test.yml --profile os-both up -d
 * 2. Wait for OpenSearch to be healthy
 * 3. Run tests: pnpm test:integration
 *
 * The tests will automatically run against all available OpenSearch versions.
 * Unavailable versions are skipped entirely.
 */

import { mock } from 'jest-mock-extended';
import type {
	ICredentialDataDecryptedObject,
	IExecuteFunctions,
	INodeExecutionData,
	IDataObject,
	INodeParameters,
	IHttpRequestOptions,
} from 'n8n-workflow';
import { Client } from '@opensearch-project/opensearch';

import { OpenSearch } from '../../nodes/OpenSearch/OpenSearch.node';
import { OPENSEARCH_CONFIG, isInstanceAvailable, getInstanceUrl } from './config';

// Conditional describe - skip entire suite if instance unavailable
const describeIf = (condition: boolean) => (condition ? describe : describe.skip);

// Helper to create test suite for a specific OpenSearch version
function createOpenSearchTestSuite(
	instanceName: string,
	instanceUrl: string,
	isAvailable: boolean
) {
	const TEST_INDEX = `integration-test-${instanceName.replace(/[^a-z0-9]/gi, '-').toLowerCase()}`;

	describeIf(isAvailable)(`${instanceName} Integration Tests`, () => {
		let client: Client;
		let nodeInstance: OpenSearch;
		let testCredentials: ICredentialDataDecryptedObject;

		beforeAll(async () => {
			nodeInstance = new OpenSearch();
		testCredentials = {
			baseUrl: instanceUrl,
			username: OPENSEARCH_CONFIG.username,
			password: OPENSEARCH_CONFIG.password,
			ignoreSSLIssues: true,
		};

		// Create real client for setup/teardown and health checks
		client = new Client({
			node: instanceUrl,
			auth: {
				username: OPENSEARCH_CONFIG.username,
				password: OPENSEARCH_CONFIG.password,
			},
			ssl: {
				rejectUnauthorized: false,
			},
		});

		// Wait for OpenSearch to be ready
		let retries = 30;
		while (retries > 0) {
			try {
				await client.cluster.health({});
				break;
			} catch {
				retries--;
				if (retries === 0) {
					throw new Error(`${instanceName} is not available at ${instanceUrl}`);
				}
				await new Promise((resolve) => setTimeout(resolve, 1000));
			}
		}

		// Clean up any leftover test index
		try {
			await client.indices.delete({ index: TEST_INDEX });
		} catch {
			// Index might not exist
		}
	}, 60000);

	afterAll(async () => {
		if (!client) return;

		// Cleanup test indices
		try {
			await client.indices.delete({ index: TEST_INDEX });
		} catch {
			// Index might not exist
		}
		await client.close();
	});

	/**
	 * Helper to create a mocked IExecuteFunctions context that allows real HTTP calls
	 */
	function createMockExecuteFunctions(
		params: Record<string, unknown>,
		inputData: INodeExecutionData[] = [{ json: {} }]
	): IExecuteFunctions {
		const mockContext = mock<IExecuteFunctions>();

		mockContext.getInputData.mockReturnValue(inputData);
		mockContext.getNodeParameter = jest.fn().mockImplementation(
			(paramName: string, _itemIndex: number, fallbackValue?: unknown) => {
				return params[paramName] ?? fallbackValue;
			}
		) as typeof mockContext.getNodeParameter;

		mockContext.getNode.mockReturnValue({
			id: 'test-node-id',
			name: 'OpenSearch',
			type: 'opensearch',
			typeVersion: 1,
			position: [0, 0],
			parameters: params as INodeParameters,
		});

		mockContext.getCredentials.mockResolvedValue(testCredentials);
		mockContext.continueOnFail.mockReturnValue(false);

		// Implement real HTTP request helper that calls OpenSearch
		mockContext.helpers = {
			constructExecutionMetaData: jest.fn().mockImplementation(
				(data: INodeExecutionData[], _options) => data
			),
			returnJsonArray: jest.fn().mockImplementation(
				(data: IDataObject | IDataObject[]) => {
					if (Array.isArray(data)) {
						return data.map((item) => ({ json: item }));
					}
					return [{ json: data }];
				}
			),
			httpRequest: jest.fn().mockImplementation(async (options: IHttpRequestOptions) => {
				// Make real HTTP calls to OpenSearch using the OpenSearch client
				// This properly handles HTTPS and authentication
				const url = new URL(options.url as string);
				const path = url.pathname + url.search;

				const requestOptions = {
					method: options.method as string,
					path,
					body: options.body ? (typeof options.body === 'string'
						? options.body
						: JSON.stringify(options.body)) : undefined,
				};

				try {
					const response = await client.transport.request(requestOptions as Parameters<typeof client.transport.request>[0]);
					// For HEAD requests, OpenSearch returns success with statusCode 404 instead of throwing
					// We need to check the status code and throw an error if it indicates failure
					if (response.statusCode && response.statusCode >= 400) {
						const error = new Error(`HTTP ${response.statusCode}`);
						(error as Error & { statusCode: number }).statusCode = response.statusCode;
						throw error;
					}
					return response.body;
				} catch (error) {
					const err = error as Error & { meta?: { body?: unknown; statusCode?: number }; statusCode?: number };
					// Re-throw if already has statusCode (from our check above)
					if (err.statusCode) {
						throw err;
					}
					if (err.meta?.statusCode) {
						const newError = new Error(`HTTP ${err.meta.statusCode}: ${JSON.stringify(err.meta.body)}`);
						(newError as Error & { statusCode: number }).statusCode = err.meta.statusCode;
						throw newError;
					}
					throw error;
				}
			}),
			httpRequestWithAuthentication: jest.fn().mockImplementation(
				async (_credentialsType: string, options: IHttpRequestOptions) => {
					// For this test, we use httpRequest directly since OpenSearch doesn't require auth
					return mockContext.helpers.httpRequest!(options);
				}
			),
			request: jest.fn().mockImplementation(async (options: IHttpRequestOptions) => {
				return mockContext.helpers.httpRequest!(options);
			}),
			requestWithAuthentication: jest.fn().mockImplementation(
				async (_credentialsType: string, options: IHttpRequestOptions) => {
					return mockContext.helpers.httpRequest!(options);
				}
			),
		} as unknown as IExecuteFunctions['helpers'];

		return mockContext;
	}

	describe('Index Operations', () => {
		describe('index:create', () => {
			it('should create an index through node execute', async () => {
				const params = {
					resource: 'index',
					operation: 'create',
					indexId: TEST_INDEX,
					additionalFields: {},
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0]).toHaveLength(1);
				expect(result[0][0].json).toHaveProperty('acknowledged', true);

				// Verify the index actually exists
				const indexExists = await client.indices.exists({ index: TEST_INDEX });
				expect(indexExists.body).toBe(true);
			});

			it('should skip creation without error when skipIfExists is true and index exists', async () => {
				// First ensure the index exists
				const indexExists = await client.indices.exists({ index: TEST_INDEX });
				expect(indexExists.body).toBe(true);

				const params = {
					resource: 'index',
					operation: 'create',
					indexId: TEST_INDEX,
					additionalFields: { skipIfExists: true },
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0]).toHaveLength(1);
				expect(result[0][0].json).toHaveProperty('acknowledged', true);
				expect(result[0][0].json).toHaveProperty('skipped', true);
			});

			it('should fail when creating existing index without skipIfExists', async () => {
				// First ensure the index exists
				const indexExists = await client.indices.exists({ index: TEST_INDEX });
				expect(indexExists.body).toBe(true);

				const params = {
					resource: 'index',
					operation: 'create',
					indexId: TEST_INDEX,
					additionalFields: {},
				};

				const mockContext = createMockExecuteFunctions(params);

				await expect(nodeInstance.execute.call(mockContext)).rejects.toThrow();
			});
		});

		describe('index:get', () => {
			it('should get index information through node execute', async () => {
				const params = {
					resource: 'index',
					operation: 'get',
					indexId: TEST_INDEX,
					additionalFields: {},
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0]).toHaveLength(1);
				expect(result[0][0].json).toHaveProperty('id', TEST_INDEX);
				expect(result[0][0].json).toHaveProperty('settings');
			});
		});

		describe('index:getAll', () => {
			it('should list all indices through node execute', async () => {
				const params = {
					resource: 'index',
					operation: 'getAll',
					returnAll: true,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				// Should include our test index
				const indexIds = result[0].map((item) => item.json.indexId);
				expect(indexIds).toContain(TEST_INDEX);
			});

			it('should limit results when returnAll is false', async () => {
				const params = {
					resource: 'index',
					operation: 'getAll',
					returnAll: false,
					limit: 1,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0].length).toBeLessThanOrEqual(1);
			});
		});
	});

	describe('Document Operations', () => {

		describe('document:create', () => {
			it('should create a document through node execute', async () => {
				const params = {
					resource: 'document',
					operation: 'create',
					indexId: TEST_INDEX,
					dataToSend: 'defineBelow',
					'fieldsUi.fieldValues': [
						{ fieldId: 'title', fieldValue: 'Test Document' },
						{ fieldId: 'content', fieldValue: 'This is test content' },
					],
					additionalFields: {},
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0]).toHaveLength(1);
				expect(result[0][0].json).toHaveProperty('result', 'created');
				expect(result[0][0].json).toHaveProperty('_id');

				// Wait for index refresh
				await client.indices.refresh({ index: TEST_INDEX });
			});

			it('should create a document with specific ID', async () => {
				const params = {
					resource: 'document',
					operation: 'create',
					indexId: TEST_INDEX,
					dataToSend: 'defineBelow',
					'fieldsUi.fieldValues': [
						{ fieldId: 'title', fieldValue: 'Specific ID Document' },
					],
					additionalFields: { documentId: 'specific-id-456' },
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0][0].json).toHaveProperty('_id', 'specific-id-456');

				// Verify document exists
				const doc = await client.get({ index: TEST_INDEX, id: 'specific-id-456' });
				expect(doc.body._source.title).toBe('Specific ID Document');
			});
		});

		describe('document:get', () => {
			it('should get a document through node execute', async () => {
				const params = {
					resource: 'document',
					operation: 'get',
					indexId: TEST_INDEX,
					documentId: 'specific-id-456',
					options: {},
					simple: false,
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0][0].json).toHaveProperty('_id', 'specific-id-456');
				expect(result[0][0].json).toHaveProperty('_source');
			});

			it('should simplify document response when simple is true', async () => {
				const params = {
					resource: 'document',
					operation: 'get',
					indexId: TEST_INDEX,
					documentId: 'specific-id-456',
					options: {},
					simple: true,
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0][0].json).toHaveProperty('_id', 'specific-id-456');
				expect(result[0][0].json).toHaveProperty('title', 'Specific ID Document');
				expect(result[0][0].json).not.toHaveProperty('_source');
			});
		});

		describe('document:update', () => {
			it('should update a document through node execute', async () => {
				const params = {
					resource: 'document',
					operation: 'update',
					indexId: TEST_INDEX,
					documentId: 'specific-id-456',
					dataToSend: 'defineBelow',
					'fieldsUi.fieldValues': [
						{ fieldId: 'title', fieldValue: 'Updated Title' },
					],
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0][0].json).toHaveProperty('result', 'updated');

				// Verify update
				const doc = await client.get({ index: TEST_INDEX, id: 'specific-id-456' });
				expect(doc.body._source.title).toBe('Updated Title');
			});
		});

		describe('document:getAll', () => {
			it('should get all documents with simple response', async () => {
				// Refresh to ensure all docs are searchable
				await client.indices.refresh({ index: TEST_INDEX });

				const params = {
					resource: 'document',
					operation: 'getAll',
					indexId: TEST_INDEX,
					returnAll: false,
					limit: 10,
					options: {},
					simple: true,
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0].length).toBeGreaterThan(0);
				// Simple response should have _id and source fields flattened
				expect(result[0][0].json).toHaveProperty('_id');
			});
		});

		describe('document:search', () => {
			it('should search documents with query', async () => {
				// Refresh to ensure all docs are searchable
				await client.indices.refresh({ index: TEST_INDEX });

				const params = {
					resource: 'document',
					operation: 'search',
					indexId: TEST_INDEX,
					query: JSON.stringify({ query: { match: { title: 'Updated' } } }),
					returnAll: false,
					limit: 10,
					options: {},
					simple: true,
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0].length).toBeGreaterThan(0);
				expect(result[0][0].json).toHaveProperty('title');
				expect((result[0][0].json.title as string).toLowerCase()).toContain('updated');
			});
		});

		describe('document:delete', () => {
			it('should delete a document through node execute', async () => {
				const params = {
					resource: 'document',
					operation: 'delete',
					indexId: TEST_INDEX,
					documentId: 'specific-id-456',
					'options.bulkOperation': false,
				};

				const mockContext = createMockExecuteFunctions(params);
				const result = await nodeInstance.execute.call(mockContext);

				expect(result[0][0].json).toHaveProperty('deleted', true);

				// Verify deletion
				await expect(
					client.get({ index: TEST_INDEX, id: 'specific-id-456' })
				).rejects.toThrow();
			});
		});
	});

	describe('index:delete', () => {
		it('should delete an index through node execute', async () => {
			// Clean up first in case of leftover from previous run
			try {
				await client.indices.delete({ index: 'temp-delete-test' });
			} catch {
				// Index might not exist
			}
			// Create a temporary index to delete
			await client.indices.create({ index: 'temp-delete-test' });

			const params = {
				resource: 'index',
				operation: 'delete',
				indexId: 'temp-delete-test',
				options: {},
			};

			const mockContext = createMockExecuteFunctions(params);
			const result = await nodeInstance.execute.call(mockContext);

			expect(result[0][0].json).toHaveProperty('deleted', true);

			// Verify deletion
			const exists = await client.indices.exists({ index: 'temp-delete-test' });
			expect(exists.body).toBe(false);
		});

		it('should skip deletion without error when skipIfNotExists is true and index does not exist', async () => {
			// Ensure the index does not exist
			try {
				await client.indices.delete({ index: 'nonexistent-skip-test' });
			} catch {
				// Index might not exist
			}

			const params = {
				resource: 'index',
				operation: 'delete',
				indexId: 'nonexistent-skip-test',
				options: { skipIfNotExists: true },
			};

			const mockContext = createMockExecuteFunctions(params);
			const result = await nodeInstance.execute.call(mockContext);

			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toHaveProperty('deleted', true);
			expect(result[0][0].json).toHaveProperty('skipped', true);
		});

		it('should fail when deleting nonexistent index without skipIfNotExists', async () => {
			// Ensure the index does not exist
			try {
				await client.indices.delete({ index: 'nonexistent-fail-test' });
			} catch {
				// Index might not exist
			}

			const params = {
				resource: 'index',
				operation: 'delete',
				indexId: 'nonexistent-fail-test',
				options: {},
			};

			const mockContext = createMockExecuteFunctions(params);

			await expect(nodeInstance.execute.call(mockContext)).rejects.toThrow();
		});
	});

	describe('Error Handling', () => {
		it('should handle document not found error', async () => {
			const params = {
				resource: 'document',
				operation: 'get',
				indexId: TEST_INDEX,
				documentId: 'nonexistent-doc-id',
				options: {},
				simple: false,
				'options.bulkOperation': false,
			};

			const mockContext = createMockExecuteFunctions(params);

			await expect(nodeInstance.execute.call(mockContext)).rejects.toThrow();
		});

		it('should handle index not found error', async () => {
			const params = {
				resource: 'index',
				operation: 'get',
				indexId: 'nonexistent-index-xyz',
				additionalFields: {},
			};

			const mockContext = createMockExecuteFunctions(params);

			await expect(nodeInstance.execute.call(mockContext)).rejects.toThrow();
		});
	});

	}); // end describeIf
} // end createOpenSearchTestSuite

// Create test suites for available OpenSearch versions
createOpenSearchTestSuite('OpenSearch 3.x', getInstanceUrl('3.x'), isInstanceAvailable('3.x'));
createOpenSearchTestSuite('OpenSearch 2.x', getInstanceUrl('2.x'), isInstanceAvailable('2.x'));
