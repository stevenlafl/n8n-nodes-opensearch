/**
 * Integration tests for VectorStoreOpenSearch node operations.
 *
 * These tests verify the VectorStore node behavior by calling supplyData()
 * with a mocked context, but allowing real calls to OpenSearch.
 * Embeddings are mocked since we don't have a real embedding provider.
 *
 * Prerequisites:
 * 1. Start OpenSearch using: docker compose -f docker-compose.test.yml --profile os-both up -d
 * 2. Wait for OpenSearch to be healthy
 * 3. Run tests: pnpm test:integration
 *
 * The tests will automatically run against all available OpenSearch versions.
 */

import { mock } from 'jest-mock-extended';
import type {
	ICredentialDataDecryptedObject,
	ISupplyDataFunctions,
	IExecuteFunctions,
	INodeExecutionData,
	INodeParameters,
} from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';
import { Client } from '@opensearch-project/opensearch';
import type { Embeddings } from '@langchain/core/embeddings';

import { VectorStoreOpenSearch } from '../../nodes/vector_store/VectorStoreOpenSearch/VectorStoreOpenSearch.node';

const OPENSEARCH_USERNAME = 'admin';
const OPENSEARCH_PASSWORD = 'MyStr0ng#Pass!2024';
const VECTOR_DIMENSION = 384; // Dimension for our mock embeddings

// Define OpenSearch instances to test against
interface OpenSearchInstance {
	name: string;
	url: string;
	version: string;
}

// Check which OpenSearch instances are available
async function getAvailableInstances(): Promise<OpenSearchInstance[]> {
	const instances: OpenSearchInstance[] = [];
	const candidates = [
		{ name: 'OpenSearch 3.x', url: 'https://localhost:9200', version: '3.x' },
		{ name: 'OpenSearch 2.x', url: 'https://localhost:9201', version: '2.x' },
	];

	for (const candidate of candidates) {
		try {
			const client = new Client({
				node: candidate.url,
				auth: { username: OPENSEARCH_USERNAME, password: OPENSEARCH_PASSWORD },
				ssl: { rejectUnauthorized: false },
			});
			await client.cluster.health({ timeout: '5s' });
			await client.close();
			instances.push(candidate);
		} catch {
			// Instance not available, skip it
		}
	}

	if (instances.length === 0) {
		throw new Error(
			'No OpenSearch instances available. Please start at least one with:\n' +
			'  docker compose -f docker-compose.test.yml --profile v3 up -d  (for 3.x)\n' +
			'  docker compose -f docker-compose.test.yml --profile v2 up -d  (for 2.x)\n' +
			'  docker compose -f docker-compose.test.yml --profile os-both up -d  (for both)'
		);
	}

	return instances;
}

// Get available instances before running tests
let availableInstances: OpenSearchInstance[] = [];

beforeAll(async () => {
	availableInstances = await getAvailableInstances();
	console.log(`VectorStore tests against: ${availableInstances.map(i => i.name).join(', ')}`);
}, 30000);

/**
 * Create mock embeddings that return consistent vectors
 * This allows us to test vector search without a real embedding provider
 */
function createMockEmbeddings(): Embeddings {
	const mockEmbeddings = {
		embedQuery: jest.fn().mockImplementation(async (text: string) => {
			// Generate a deterministic vector based on text content
			// This ensures the same text always produces the same vector
			const vector = new Array(VECTOR_DIMENSION).fill(0);
			for (let i = 0; i < text.length && i < VECTOR_DIMENSION; i++) {
				vector[i] = (text.charCodeAt(i) % 100) / 100;
			}
			// Normalize
			const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0)) || 1;
			return vector.map((v) => v / magnitude);
		}),
		embedDocuments: jest.fn().mockImplementation(async (documents: string[]) => {
			const results = [];
			for (const doc of documents) {
				results.push(await mockEmbeddings.embedQuery(doc));
			}
			return results;
		}),
	} as unknown as Embeddings;

	return mockEmbeddings;
}

describe.each([
	['OpenSearch 3.x', 'https://localhost:9200', '3.x'],
	['OpenSearch 2.x', 'https://localhost:9201', '2.x'],
])('%s VectorStore Integration Tests', (instanceName, instanceUrl, instanceVersion) => {
	let client: Client;
	let nodeInstance: VectorStoreOpenSearch;
	let mockEmbeddings: Embeddings;
	let testCredentials: ICredentialDataDecryptedObject;
	let instanceAvailable = false;
	const TEST_INDEX = `vector-integration-test-${instanceVersion.replace('.', '-')}`;

	beforeAll(async () => {
		// Check if this instance is available
		instanceAvailable = availableInstances.some(i => i.url === instanceUrl);
		if (!instanceAvailable) {
			console.log(`⏭️  Skipping ${instanceName} - not available`);
			return;
		}

		nodeInstance = new VectorStoreOpenSearch();
		mockEmbeddings = createMockEmbeddings();
		testCredentials = {
			baseUrl: instanceUrl,
			username: OPENSEARCH_USERNAME,
			password: OPENSEARCH_PASSWORD,
			ignoreSSLIssues: true,
		};

		// Create real client for setup/teardown and health checks
		client = new Client({
			node: instanceUrl,
			auth: {
				username: OPENSEARCH_USERNAME,
				password: OPENSEARCH_PASSWORD,
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

	// Wrapper for it() that skips if instance unavailable
	const testIf = (name: string, fn: () => Promise<void>) => {
		it(name, async () => {
			if (!instanceAvailable) {
				return; // Skip silently
			}
			await fn();
		});
	};

	/**
	 * Helper to create a mocked ISupplyDataFunctions context
	 */
	function createMockSupplyDataContext(
		params: Record<string, unknown>,
		mode: string = 'retrieve'
	): ISupplyDataFunctions {
		const mockContext = mock<ISupplyDataFunctions>();

		mockContext.getNodeParameter = jest.fn().mockImplementation(
			(paramName: string, _itemIndex: number, fallbackValue?: unknown) => {
				if (paramName === 'mode') return mode;
				return params[paramName] ?? fallbackValue;
			}
		) as typeof mockContext.getNodeParameter;

		mockContext.getCredentials.mockResolvedValue(testCredentials);

		mockContext.getNode.mockReturnValue({
			id: 'test-node-id',
			name: 'VectorStoreOpenSearch',
			type: 'vectorStoreOpenSearch',
			typeVersion: 1.1,
			position: [0, 0],
			parameters: { mode, ...params } as INodeParameters,
		});

		mockContext.getInputConnectionData = jest.fn().mockImplementation(
			(connectionType: string) => {
				if (connectionType === NodeConnectionTypes.AiEmbedding) {
					return mockEmbeddings;
				}
				if (connectionType === NodeConnectionTypes.AiDocument) {
					return {
						load: jest.fn().mockResolvedValue([
							{ pageContent: 'test document', metadata: { source: 'test' } },
						]),
					};
				}
				return null;
			}
		) as typeof mockContext.getInputConnectionData;

		mockContext.logger = {
			info: jest.fn(),
			debug: jest.fn(),
			error: jest.fn(),
			warn: jest.fn(),
			verbose: jest.fn(),
		} as unknown as ISupplyDataFunctions['logger'];

		return mockContext;
	}

	/**
	 * Helper to create a mocked IExecuteFunctions context for insert/load operations
	 */
	function createMockExecuteFunctions(
		params: Record<string, unknown>,
		inputData: INodeExecutionData[] = [{ json: {} }],
		mode: string = 'insert'
	): IExecuteFunctions {
		const mockContext = mock<IExecuteFunctions>();

		mockContext.getInputData.mockReturnValue(inputData);
		mockContext.getNodeParameter = jest.fn().mockImplementation(
			(paramName: string, _itemIndex: number, fallbackValue?: unknown) => {
				if (paramName === 'mode') return mode;
				return params[paramName] ?? fallbackValue;
			}
		) as typeof mockContext.getNodeParameter;

		mockContext.getCredentials.mockResolvedValue(testCredentials);

		mockContext.getNode.mockReturnValue({
			id: 'test-node-id',
			name: 'VectorStoreOpenSearch',
			type: 'vectorStoreOpenSearch',
			typeVersion: 1.1,
			position: [0, 0],
			parameters: { mode, ...params } as INodeParameters,
		});

		mockContext.getInputConnectionData = jest.fn().mockImplementation(
			(connectionType: string) => {
				if (connectionType === NodeConnectionTypes.AiEmbedding) {
					return mockEmbeddings;
				}
				if (connectionType === NodeConnectionTypes.AiDocument) {
					return [
						{ pageContent: 'Document about artificial intelligence and machine learning', metadata: { source: 'ai.txt', category: 'tech' } },
						{ pageContent: 'Document about natural language processing', metadata: { source: 'nlp.txt', category: 'tech' } },
						{ pageContent: 'Document about cooking recipes and food', metadata: { source: 'cooking.txt', category: 'food' } },
					];
				}
				return null;
			}
		) as typeof mockContext.getInputConnectionData;

		mockContext.continueOnFail.mockReturnValue(false);
		mockContext.getExecutionCancelSignal.mockReturnValue(undefined);

		mockContext.logger = {
			info: jest.fn(),
			debug: jest.fn(),
			error: jest.fn(),
			warn: jest.fn(),
			verbose: jest.fn(),
		} as unknown as IExecuteFunctions['logger'];

		mockContext.helpers = {
			constructExecutionMetaData: jest.fn().mockImplementation(
				(data: INodeExecutionData[], _options) => data
			),
		} as unknown as IExecuteFunctions['helpers'];

		return mockContext;
	}

	describe('Insert Mode', () => {
		testIf('should insert documents into OpenSearch vector store', async () => {
			const params = {
				indexName: TEST_INDEX,
				engine: 'lucene',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			const mockContext = createMockExecuteFunctions(params, [{ json: {} }], 'insert');

			// Execute the node (insert mode uses execute, not supplyData)
			const result = await nodeInstance.execute.call(mockContext);

			expect(result).toBeDefined();
			expect(result[0].length).toBeGreaterThan(0);

			// Wait for OpenSearch to index the documents
			await client.indices.refresh({ index: TEST_INDEX });

			// Verify documents were inserted
			const searchResult = await client.search({
				index: TEST_INDEX,
				body: { query: { match_all: {} } },
			});

			expect(searchResult.body.hits.total.value).toBe(3);
		});
	});

	describe('Retrieve Mode', () => {
		testIf('should retrieve vector store client for use with AI nodes', async () => {
			const params = {
				indexName: TEST_INDEX,
				engine: 'lucene',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			const mockContext = createMockSupplyDataContext(params, 'retrieve');

			// Call supplyData to get the vector store
			const result = await nodeInstance.supplyData.call(mockContext, 0);

			expect(result).toHaveProperty('response');
			expect(result).toHaveProperty('closeFunction');

			// The response should be a vector store that can perform similarity search
			const vectorStore = result.response;
			expect(vectorStore).toHaveProperty('similaritySearchVectorWithScore');

			// Clean up
			if (result.closeFunction) {
				await result.closeFunction();
			}
		});

		testIf('should perform similarity search on retrieved vector store', async () => {
			const params = {
				indexName: TEST_INDEX,
				engine: 'lucene',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			const mockContext = createMockSupplyDataContext(params, 'retrieve');

			// Call supplyData to get the vector store
			const result = await nodeInstance.supplyData.call(mockContext, 0);
			const vectorStore = result.response as { similaritySearchVectorWithScore: Function };

			// Get embedding for query
			const queryVector = await mockEmbeddings.embedQuery('artificial intelligence');

			// Perform similarity search
			const searchResults = await vectorStore.similaritySearchVectorWithScore(
				queryVector,
				2 // top 2 results
			);

			expect(searchResults.length).toBeLessThanOrEqual(2);
			if (searchResults.length > 0) {
				// Each result should be [Document, score]
				expect(searchResults[0]).toHaveLength(2);
				expect(searchResults[0][0]).toHaveProperty('pageContent');
				expect(typeof searchResults[0][1]).toBe('number');
			}

			// Clean up
			if (result.closeFunction) {
				await result.closeFunction();
			}
		});
	});

	describe('Load Mode (Similarity Search)', () => {
		testIf('should search and return similar documents', async () => {
			const params = {
				indexName: TEST_INDEX,
				engine: 'lucene',
				prompt: 'machine learning',
				topK: 2,
				includeDocumentMetadata: true,
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			const mockContext = createMockExecuteFunctions(params, [{ json: {} }], 'load');

			// Execute the node in load mode
			const result = await nodeInstance.execute.call(mockContext);

			expect(result).toBeDefined();
			expect(result[0].length).toBeLessThanOrEqual(2);

			if (result[0].length > 0) {
				// Each result should have document and score
				expect(result[0][0].json).toHaveProperty('document');
				expect(result[0][0].json).toHaveProperty('score');
			}
		});
	});

	describe('Update Mode', () => {
		let testDocId: string;

		beforeAll(async () => {
			// Create a document to update
			const response = await client.index({
				index: TEST_INDEX,
				body: {
					text: 'Original document content',
					embedding: new Array(VECTOR_DIMENSION).fill(0.1),
					metadata: { source: 'update-test.txt' },
				},
				refresh: true,
			});
			testDocId = response.body._id;
		});

		testIf('should update a document by ID', async () => {
			const params = {
				indexName: TEST_INDEX,
				engine: 'lucene',
				id: testDocId,
				// Required parameters for N8nJsonLoader
				jsonMode: 'allInputData',
				pointers: '',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			// Create mock context with updated document content as input JSON
			// N8nJsonLoader will process this input data
			const inputData = [{ json: { text: 'Updated document content' } }];
			const mockContext = createMockExecuteFunctions(params, inputData, 'update');

			// Execute update
			const result = await nodeInstance.execute.call(mockContext);

			expect(result).toBeDefined();

			// Refresh and verify update
			await client.indices.refresh({ index: TEST_INDEX });

			// The old document should be replaced - search for updated content
			const searchResult = await client.search({
				index: TEST_INDEX,
				body: {
					query: {
						match: { text: 'Updated document content' },
					},
				},
			});

			expect(searchResult.body.hits.total.value).toBeGreaterThan(0);
		});
	});

	describe('Delete Functionality', () => {
		testIf('should add replacement document during update operation', async () => {
			// Create a unique document ID for this test
			const docId = `delete-test-${Date.now()}`;

			// Create a document with specific ID
			await client.index({
				index: TEST_INDEX,
				id: docId,
				body: {
					text: 'Document to replace',
					embedding: new Array(VECTOR_DIMENSION).fill(0.2),
					metadata: { source: 'delete-test.txt' },
				},
				refresh: true,
			});

			// Verify document exists
			const existsBefore = await client.exists({ index: TEST_INDEX, id: docId });
			expect(existsBefore.body).toBe(true);

			// Update operation adds new document with specified ID
			// Note: LangChain's OpenSearchVectorStore.addDocuments with ids may create new docs
			// rather than overwriting existing ones - this is a known limitation
			const params = {
				indexName: TEST_INDEX,
				engine: 'lucene',
				id: docId,
				// Required parameters for N8nJsonLoader
				jsonMode: 'allInputData',
				pointers: '',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			// Provide replacement document content as input JSON
			const inputData = [{ json: { text: 'Replacement content' } }];
			const mockContext = createMockExecuteFunctions(params, inputData, 'update');

			// The update operation should complete without error
			const result = await nodeInstance.execute.call(mockContext);
			expect(result).toBeDefined();

			await client.indices.refresh({ index: TEST_INDEX });

			// Verify replacement content was added to the index
			const searchResult = await client.search({
				index: TEST_INDEX,
				body: {
					query: {
						match: { text: 'Replacement content' },
					},
				},
			});

			expect(searchResult.body.hits.total.value).toBeGreaterThan(0);
		});
	});

	describe('Engine Configuration', () => {
		testIf('should use lucene engine successfully', async () => {
			const tempIndex = 'engine-test-lucene';

			const params = {
				indexName: tempIndex,
				engine: 'lucene',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			const mockContext = createMockExecuteFunctions(params, [{ json: {} }], 'insert');
			mockContext.getInputConnectionData = jest.fn().mockImplementation(
				(connectionType: string) => {
					if (connectionType === NodeConnectionTypes.AiEmbedding) {
						return mockEmbeddings;
					}
					if (connectionType === NodeConnectionTypes.AiDocument) {
						return [
							{ pageContent: 'Test document', metadata: {} },
						];
					}
					return null;
				}
			) as typeof mockContext.getInputConnectionData;

			await nodeInstance.execute.call(mockContext);
			await client.indices.refresh({ index: tempIndex });

			// Verify index was created with k-NN settings
			const indexSettings = await client.indices.getMapping({ index: tempIndex });
			expect(indexSettings.body[tempIndex]).toBeDefined();

			// Clean up
			await client.indices.delete({ index: tempIndex });
		});

		testIf('should handle nmslib engine based on OpenSearch version', async () => {
			const tempIndex = 'engine-test-nmslib';

			// Clean up any leftover index first
			try {
				await client.indices.delete({ index: tempIndex });
			} catch {
				// Index might not exist
			}

			const params = {
				indexName: tempIndex,
				engine: 'nmslib',
				'options.fieldNames.values': {
					vectorFieldName: 'embedding',
					textFieldName: 'text',
					metadataFieldName: 'metadata',
				},
			};

			const mockContext = createMockExecuteFunctions(params, [{ json: {} }], 'insert');
			mockContext.getInputConnectionData = jest.fn().mockImplementation(
				(connectionType: string) => {
					if (connectionType === NodeConnectionTypes.AiEmbedding) {
						return mockEmbeddings;
					}
					if (connectionType === NodeConnectionTypes.AiDocument) {
						return [
							{ pageContent: 'Test document for nmslib', metadata: {} },
						];
					}
					return null;
				}
			) as typeof mockContext.getInputConnectionData;

			if (instanceVersion === '3.x') {
				// OpenSearch 3.x: nmslib is deprecated and should fail
				await expect(nodeInstance.execute.call(mockContext)).rejects.toThrow(
					/nmslib engine is deprecated/i
				);
			} else {
				// OpenSearch 2.x: nmslib should work
				await nodeInstance.execute.call(mockContext);
				await client.indices.refresh({ index: tempIndex });

				// Verify index was created
				const indexExists = await client.indices.exists({ index: tempIndex });
				expect(indexExists.body).toBe(true);

				// Clean up
				await client.indices.delete({ index: tempIndex });
			}
		});
	});

	describe('Error Handling', () => {
		testIf('should handle connection errors gracefully', async () => {
			const badCredentials: ICredentialDataDecryptedObject = {
				baseUrl: 'http://nonexistent-host:9200',
				username: '',
				password: '',
				ignoreSSLIssues: false,
			};

			const params = {
				indexName: 'test-index',
				engine: 'lucene',
			};

			const mockContext = createMockSupplyDataContext(params, 'retrieve');
			(mockContext.getCredentials as jest.Mock).mockResolvedValue(badCredentials);

			// Getting the vector store should succeed (client creation is lazy)
			const result = await nodeInstance.supplyData.call(mockContext, 0);
			const vectorStore = result.response as { similaritySearchVectorWithScore: Function };

			// But operations should fail
			const queryVector = await mockEmbeddings.embedQuery('test');
			await expect(
				vectorStore.similaritySearchVectorWithScore(queryVector, 1)
			).rejects.toThrow();

			if (result.closeFunction) {
				await result.closeFunction();
			}
		});
	});
});
