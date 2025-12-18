import { mock, mockDeep } from 'jest-mock-extended';
import type {
	IExecuteFunctions,
	IDataObject,
	ICredentialDataDecryptedObject,
} from 'n8n-workflow';

/**
 * Creates a mock IExecuteFunctions using jest-mock-extended.
 * This follows n8n's own testing patterns.
 *
 * Usage:
 *   const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
 *   executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
 *   executeFunctions.getNodeParameter.calledWith('indexName', 0).mockReturnValue('my-index');
 */
export function createMockExecuteFunctions() {
	const httpRequestMock = jest.fn();

	// Use mockDeep to create deep mocks of nested objects like helpers
	const executeFunctions = mockDeep<IExecuteFunctions>();

	// Set up common defaults
	executeFunctions.getNode.mockReturnValue({
		id: 'test-node-id',
		name: 'OpenSearch Test Node',
		type: 'n8n-nodes-opensearch.openSearch',
		typeVersion: 1,
		position: [0, 0],
		parameters: {},
	});
	executeFunctions.continueOnFail.mockReturnValue(false);

	// Wire up the http request mock to helpers
	executeFunctions.helpers.httpRequestWithAuthentication.mockImplementation(httpRequestMock);

	// Set up helper methods
	executeFunctions.helpers.constructExecutionMetaData.mockImplementation((data, meta) =>
		data.map((item: { json: IDataObject }) => ({ ...item, pairedItem: meta.itemData }))
	);

	executeFunctions.helpers.returnJsonArray.mockImplementation((data: IDataObject | IDataObject[]) => {
		const arr = Array.isArray(data) ? data : [data];
		return arr.map(item => ({ json: item }));
	});

	return { executeFunctions, httpRequestMock };
}

/**
 * Creates mock credentials for OpenSearch API
 */
export function createMockCredentials(overrides?: Partial<ICredentialDataDecryptedObject>): ICredentialDataDecryptedObject {
	return mock<ICredentialDataDecryptedObject>({
		username: 'admin',
		password: 'admin',
		baseUrl: 'https://localhost:9200',
		ignoreSSLIssues: true,
		...overrides,
	});
}

/**
 * Creates mock OpenSearch search response
 */
export function createMockSearchResponse(hits: IDataObject[], total = hits.length) {
	return {
		took: 5,
		timed_out: false,
		_shards: {
			total: 1,
			successful: 1,
			skipped: 0,
			failed: 0,
		},
		hits: {
			total: {
				value: total,
				relation: 'eq',
			},
			max_score: 1.0,
			hits: hits.map((hit, index) => ({
				_index: 'test-index',
				_id: `doc-${index}`,
				_score: 1.0,
				_source: hit,
			})),
		},
	};
}

/**
 * Creates mock OpenSearch index response
 */
export function createMockIndexResponse(indexId: string) {
	return {
		acknowledged: true,
		shards_acknowledged: true,
		index: indexId,
	};
}

/**
 * Creates mock OpenSearch document response
 */
export function createMockDocumentResponse(indexId: string, documentId: string, source: IDataObject) {
	return {
		_index: indexId,
		_id: documentId,
		_version: 1,
		_seq_no: 0,
		_primary_term: 1,
		found: true,
		_source: source,
	};
}

/**
 * Creates mock OpenSearch bulk response
 */
export function createMockBulkResponse(items: Array<{ action: string; _id: string; status: number }>) {
	return {
		took: 10,
		errors: items.some(item => item.status >= 400),
		items: items.map(item => ({
			[item.action]: {
				_index: 'test-index',
				_id: item._id,
				_version: 1,
				result: item.status < 400 ? 'created' : 'error',
				status: item.status,
			},
		})),
	};
}

/**
 * Creates mock embeddings for vector store tests
 */
export function createMockEmbeddings() {
	return {
		embedQuery: jest.fn().mockResolvedValue(new Array(1536).fill(0.1)),
		embedDocuments: jest.fn().mockResolvedValue([new Array(1536).fill(0.1)]),
	};
}
