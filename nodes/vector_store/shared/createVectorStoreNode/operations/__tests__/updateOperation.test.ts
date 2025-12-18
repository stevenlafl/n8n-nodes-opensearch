/* eslint-disable @typescript-eslint/unbound-method */
import type { Document } from '@langchain/core/documents';
import type { Embeddings } from '@langchain/core/embeddings';
import type { VectorStore } from '@langchain/core/vectorstores';
import type { MockProxy } from 'jest-mock-extended';
import { mock } from 'jest-mock-extended';
import type { IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

import { logAiEvent } from '../../../../../../utils/helpers';

import type { VectorStoreNodeConstructorArgs } from '../../types';
import { isUpdateSupported } from '../../utils';
import { handleUpdateOperation } from '../updateOperation';

// Mock dependencies
jest.mock('../../utils', () => ({
	isUpdateSupported: jest.fn(),
}));

jest.mock('../../../../../../utils/helpers', () => ({
	logAiEvent: jest.fn(),
}));

jest.mock('../../../processDocuments', () => ({
	processDocument: jest.fn().mockImplementation((_documentInput, _itemData, itemIndex) => {
		const mockProcessed = [
			{
				pageContent: `updated content ${itemIndex}`,
				metadata: { source: 'test-update' },
			} as Document,
		];

		const mockSerialized = [
			{
				json: {
					pageContent: `updated content ${itemIndex}`,
					metadata: { source: 'test-update' },
				},
				pairedItem: { item: itemIndex },
			},
		];

		return {
			processedDocuments: mockProcessed,
			serializedDocuments: mockSerialized,
		};
	}),
}));

describe('handleUpdateOperation', () => {
	let mockContext: MockProxy<IExecuteFunctions>;
	let mockEmbeddings: MockProxy<Embeddings>;
	let mockVectorStore: MockProxy<VectorStore>;
	let mockArgs: VectorStoreNodeConstructorArgs<VectorStore>;
	let mockInputItems: INodeExecutionData[];

	beforeEach(() => {
		// Mock isUpdateSupported to return true by default
		(isUpdateSupported as jest.Mock).mockReturnValue(true);

		// Mock input items
		mockInputItems = [{ json: { text: 'test document 1' } }, { json: { text: 'test document 2' } }];

		// Setup context mock
		mockContext = mock<IExecuteFunctions>();
		mockContext.getInputData.mockReturnValue(mockInputItems);
		mockContext.getNodeParameter = jest.fn().mockImplementation((paramName: string, itemIndex: number) => {
			if (paramName === 'id') {
				return `doc-id-${itemIndex}`;
			}
			if (paramName === 'embeddingBatchSize') {
				return 200;
			}
			return undefined;
		}) as typeof mockContext.getNodeParameter;
		(mockContext.getExecutionCancelSignal as jest.Mock).mockReturnValue({ aborted: false });

		// Setup embeddings mock
		mockEmbeddings = mock<Embeddings>();
		mockEmbeddings.embedDocuments.mockResolvedValue([[0.1, 0.2, 0.3]]);

		// Setup vector store mock
		mockVectorStore = mock<VectorStore>();
		mockVectorStore.addDocuments.mockResolvedValue(undefined);
		mockVectorStore.addVectors.mockResolvedValue(undefined);

		// Setup args mock
		mockArgs = {
			meta: {
				displayName: 'Test Vector Store',
				name: 'testVectorStore',
				description: 'Vector store for testing',
				docsUrl: 'https://example.com',
				icon: 'file:testIcon.svg',
				operationModes: ['load', 'insert', 'retrieve', 'retrieve-as-tool', 'update'],
			},
			sharedFields: [],
			getVectorStoreClient: jest.fn().mockResolvedValue(mockVectorStore),
			populateVectorStore: jest.fn().mockResolvedValue(undefined),
			releaseVectorStoreClient: jest.fn(),
		};
	});

	afterEach(() => {
		jest.clearAllMocks();
	});

	it('should throw error if update is not supported', async () => {
		// Mock isUpdateSupported to return false
		(isUpdateSupported as jest.Mock).mockReturnValue(false);

		await expect(handleUpdateOperation(mockContext, mockArgs, mockEmbeddings)).rejects.toThrow(
			NodeOperationError,
		);

		expect(mockArgs.getVectorStoreClient).not.toHaveBeenCalled();
	});

	it('should update documents with their IDs', async () => {
		// Return vectors for both documents
		mockEmbeddings.embedDocuments.mockResolvedValue([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]);

		const result = await handleUpdateOperation(mockContext, mockArgs, mockEmbeddings);

		// Should process all items
		expect(result).toHaveLength(2);

		// Should get vector store client once (batched processing)
		expect(mockArgs.getVectorStoreClient).toHaveBeenCalledTimes(1);

		// Should call addVectors with all documents and IDs in one batch
		expect(mockVectorStore.addVectors).toHaveBeenCalledTimes(1);

		// Should include both IDs in the call
		expect(mockVectorStore.addVectors).toHaveBeenCalledWith(
			[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
			expect.arrayContaining([
				expect.objectContaining({ pageContent: 'test document 1' }),
				expect.objectContaining({ pageContent: 'test document 2' }),
			]),
			{ ids: ['doc-id-0', 'doc-id-1'] },
		);

		// Should log AI event once for the batch
		expect(logAiEvent).toHaveBeenCalledTimes(1);
		expect(logAiEvent).toHaveBeenCalledWith(mockContext, 'ai-vector-store-updated');
	});

	it('should release vector store client even if update fails', async () => {
		// Mock addVectors to fail
		mockVectorStore.addVectors.mockRejectedValue(new Error('Update failed'));

		await expect(handleUpdateOperation(mockContext, mockArgs, mockEmbeddings)).rejects.toThrow(
			'Failed to insert documents: Update failed',
		);

		// Should still release the client
		expect(mockArgs.releaseVectorStoreClient).toHaveBeenCalledWith(mockVectorStore);
	});

	it('should use proper document ID from node parameters', async () => {
		// Setup custom document IDs - the implementation calls getNodeParameter for each item
		mockContext.getNodeParameter = jest.fn().mockImplementation((paramName: string, itemIndex: number) => {
			if (paramName === 'id') {
				return itemIndex === 0 ? 'custom-id-123' : 'custom-id-456';
			}
			if (paramName === 'embeddingBatchSize') {
				return 200;
			}
			return undefined;
		}) as typeof mockContext.getNodeParameter;

		// Return vectors for both documents
		mockEmbeddings.embedDocuments.mockResolvedValue([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]);

		await handleUpdateOperation(mockContext, mockArgs, mockEmbeddings);

		// Should call addVectors with both custom IDs
		expect(mockVectorStore.addVectors).toHaveBeenCalledWith(
			expect.anything(),
			expect.anything(),
			{ ids: ['custom-id-123', 'custom-id-456'] },
		);
	});

	it('should return serialized documents with pairedItem', async () => {
		// Return vectors for both documents
		mockEmbeddings.embedDocuments.mockResolvedValue([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]);

		const result = await handleUpdateOperation(mockContext, mockArgs, mockEmbeddings);

		expect(result[0].pairedItem).toEqual({ item: 0 });
		expect(result[1].pairedItem).toEqual({ item: 1 });
	});

	it('should release vector store client once after batch processing', async () => {
		// Return vectors for both documents
		mockEmbeddings.embedDocuments.mockResolvedValue([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]);

		await handleUpdateOperation(mockContext, mockArgs, mockEmbeddings);

		// Should release client once at the end (batched processing)
		expect(mockArgs.releaseVectorStoreClient).toHaveBeenCalledTimes(1);
	});
});
