import {
	createMockExecuteFunctions,
	createMockCredentials,
	createMockSearchResponse,
} from '../../tests/mocks/n8nMocks';

// Mock the modules before importing
jest.mock('n8n-workflow', () => ({
	NodeApiError: class NodeApiError extends Error {
		constructor(node: unknown, error: unknown) {
			super(typeof error === 'object' && error !== null && 'message' in error
				? String((error as { message: string }).message)
				: 'API Error');
			this.name = 'NodeApiError';
		}
	},
}));

import {
	openSearchApiRequest,
	openSearchBulkApiRequest,
	openSearchApiRequestAllItems,
	openSearchApiRequestWithScroll,
} from './GenericFunctions';

describe('GenericFunctions', () => {
	describe('openSearchApiRequest', () => {
		it('should make a GET request with correct parameters', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
			httpRequestMock.mockResolvedValue({ status: 'ok' });

			const result = await openSearchApiRequest.call(
				executeFunctions,
				'GET',
				'/test-index/_doc/1'
			);

			expect(result).toEqual({ status: 'ok' });
			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					method: 'GET',
					url: 'https://localhost:9200/test-index/_doc/1',
					json: true,
					skipSslCertificateValidation: true,
				})
			);
		});

		it('should make a POST request with body', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
			httpRequestMock.mockResolvedValue({ _id: 'doc-1' });

			const body = { title: 'Test Document', content: 'Hello World' };
			const result = await openSearchApiRequest.call(
				executeFunctions,
				'POST',
				'/test-index/_doc',
				body
			);

			expect(result).toEqual({ _id: 'doc-1' });
			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					method: 'POST',
					body,
				})
			);
		});

		it('should include query string parameters', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
			httpRequestMock.mockResolvedValue({});

			await openSearchApiRequest.call(
				executeFunctions,
				'GET',
				'/test-index/_search',
				{},
				{ size: 10, from: 0 }
			);

			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					qs: { size: 10, from: 0 },
				})
			);
		});

		it('should omit empty body and qs', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
			httpRequestMock.mockResolvedValue({});

			await openSearchApiRequest.call(executeFunctions, 'GET', '/test-index');

			const mock = executeFunctions.helpers.httpRequestWithAuthentication as jest.Mock;
			const callArgs = mock.mock.calls[0][1];
			expect(callArgs.body).toBeUndefined();
			expect(callArgs.qs).toBeUndefined();
		});

		it('should throw NodeApiError on failure', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
			httpRequestMock.mockRejectedValue(new Error('Connection refused'));

			await expect(
				openSearchApiRequest.call(executeFunctions, 'GET', '/test-index')
			).rejects.toThrow();
		});
	});

	describe('openSearchBulkApiRequest', () => {
		it('should send bulk operations with correct content type', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);

			const bulkResponse = {
				body: {
					took: 10,
					errors: false,
					items: [
						{ index: { _index: 'test', _id: '1', status: 201 } },
						{ index: { _index: 'test', _id: '2', status: 201 } },
					],
				},
				statusCode: 200,
			};
			httpRequestMock.mockResolvedValue(bulkResponse);

			const bulkBody = {
				0: '{"index":{"_index":"test","_id":"1"}}\n{"title":"Doc 1"}',
				1: '{"index":{"_index":"test","_id":"2"}}\n{"title":"Doc 2"}',
			};

			const result = await openSearchBulkApiRequest.call(executeFunctions, bulkBody);

			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					method: 'POST',
					headers: { 'Content-Type': 'application/x-ndjson' },
					url: 'https://localhost:9200/_bulk',
				})
			);
			expect(result).toHaveLength(2);
		});

		it('should handle bulk operation errors', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);

			const bulkResponse = {
				body: {
					error: { type: 'mapper_parsing_exception', reason: 'Invalid field' },
				},
				statusCode: 400,
			};
			httpRequestMock.mockResolvedValue(bulkResponse);

			await expect(
				openSearchBulkApiRequest.call(executeFunctions, { 0: 'invalid' })
			).rejects.toThrow();
		});
	});

	describe('openSearchApiRequestWithScroll', () => {
		it('should paginate through results using scroll API', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);

			const firstResponse = {
				...createMockSearchResponse([{ title: 'Doc 1' }, { title: 'Doc 2' }]),
				_scroll_id: 'scroll-id-1',
			};

			const secondResponse = {
				...createMockSearchResponse([{ title: 'Doc 3' }]),
				_scroll_id: 'scroll-id-2',
			};

			const emptyResponse = createMockSearchResponse([]);

			httpRequestMock
				.mockResolvedValueOnce(firstResponse)
				.mockResolvedValueOnce(secondResponse)
				.mockResolvedValueOnce(emptyResponse)
				.mockResolvedValueOnce({}); // DELETE scroll

			const result = await openSearchApiRequestWithScroll.call(
				executeFunctions,
				'test-index',
				{},
				{},
				1
			);

			expect(result).toHaveLength(3);
			// Verify scroll cleanup was called
			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					method: 'DELETE',
					url: expect.stringContaining('/_search/scroll'),
				})
			);
		});

		it('should handle empty results', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);
			httpRequestMock.mockResolvedValue(createMockSearchResponse([]));

			const result = await openSearchApiRequestWithScroll.call(
				executeFunctions,
				'test-index'
			);

			expect(result).toEqual([]);
		});
	});

	describe('openSearchApiRequestAllItems', () => {
		it('should paginate through results using PIT', async () => {
			const { executeFunctions, httpRequestMock } = createMockExecuteFunctions();
			const credentials = createMockCredentials();

			executeFunctions.getCredentials.calledWith('openSearchApi').mockResolvedValue(credentials);

			// PIT creation response
			const pitResponse = { id: 'pit-id-123' };

			// First search response - need to include sort array on hits
			const firstSearchResponse = createMockSearchResponse([{ title: 'Doc 1' }]);
			// Add sort to the hits
			(firstSearchResponse.hits.hits[0] as Record<string, unknown>).sort = ['sort-value-1'];
			const firstResponse = {
				...firstSearchResponse,
				pit_id: 'pit-id-123',
			};

			// Second search response (empty - end of results)
			const secondResponse = {
				...createMockSearchResponse([]),
				pit_id: 'pit-id-123',
			};

			httpRequestMock
				.mockResolvedValueOnce(pitResponse)      // Create PIT
				.mockResolvedValueOnce(firstResponse)   // First search
				.mockResolvedValueOnce(secondResponse)  // Second search (empty)
				.mockResolvedValueOnce({});             // Delete PIT

			const result = await openSearchApiRequestAllItems.call(
				executeFunctions,
				'test-index',
				{},
				{ sort: '_id:asc' }
			);

			expect(result).toHaveLength(1);
			// Verify PIT creation
			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					method: 'POST',
					url: expect.stringContaining('/_pit'),
				})
			);
			// Verify PIT cleanup
			expect(executeFunctions.helpers.httpRequestWithAuthentication).toHaveBeenCalledWith(
				'openSearchApi',
				expect.objectContaining({
					method: 'DELETE',
					url: expect.stringContaining('/_pit'),
				})
			);
		});
	});
});
