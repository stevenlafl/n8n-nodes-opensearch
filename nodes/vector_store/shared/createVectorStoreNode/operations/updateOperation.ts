/* eslint-disable @n8n/community-nodes/no-restricted-imports, @typescript-eslint/ban-ts-comment */
// @ts-nocheck
import { Document } from '@langchain/core/documents';
import type { Embeddings } from '@langchain/core/embeddings';
import type { VectorStore } from '@langchain/core/vectorstores';
import type { IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

import { logAiEvent } from '../../../../../utils/helpers';

import type { VectorStoreNodeConstructorArgs } from '../types';
import { isUpdateSupported } from '../utils';

interface DocumentWithId {
	id: string;
	document: Document;
	textContent: string;
	metadata: Record<string, unknown>;
	itemIndex: number;
}

/**
 * Handles the 'update' operation mode
 * Updates existing documents in the vector store by ID
 * Uses delete-then-insert pattern to work around LangChain limitations
 *
 * Expects input items with:
 * - _id (or id field specified by parameter): the document ID to update
 * - text (or pageContent): the text content to embed
 * - metadata (optional): document metadata
 */
export async function handleUpdateOperation<T extends VectorStore = VectorStore>(
	context: IExecuteFunctions,
	args: VectorStoreNodeConstructorArgs<T>,
	embeddings: Embeddings,
): Promise<INodeExecutionData[]> {
	// First check if update operation is supported by this vector store
	if (!isUpdateSupported(args)) {
		throw new NodeOperationError(
			context.getNode(),
			'Update operation is not implemented for this Vector Store',
		);
	}

	// Get input items
	const items = context.getInputData();

	const resultData: INodeExecutionData[] = [];
	const documentsToUpdate: DocumentWithId[] = [];

	// First pass: collect all documents and their IDs
	for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
		// Check if execution is being cancelled
		if (context.getExecutionCancelSignal()?.aborted) {
			break;
		}

		const itemData = items[itemIndex];
		const json = itemData.json;

		// Get the document ID to update - from parameter expression (e.g., {{ $json._id }})
		const documentId = context.getNodeParameter('id', itemIndex, '', {
			extractValue: true,
		}) as string;

		if (!documentId) {
			throw new NodeOperationError(
				context.getNode(),
				`Missing document ID for item ${itemIndex}. Ensure the ID parameter is set correctly.`,
				{ itemIndex },
			);
		}

		// Get text content from the item - check common field names
		const textContent = (json.text || json.pageContent || json.content) as string;

		if (!textContent) {
			throw new NodeOperationError(
				context.getNode(),
				`Missing text content for item ${itemIndex}. Expected 'text', 'pageContent', or 'content' field.`,
				{ itemIndex },
			);
		}

		// Get metadata from the item (optional)
		const metadata = (json.metadata || {}) as Record<string, unknown>;

		// Create a Document from the input item
		const document = new Document({
			pageContent: textContent,
			metadata,
		});

		documentsToUpdate.push({
			id: documentId,
			document,
			textContent,
			metadata,
			itemIndex,
		});

		// Add to result data
		resultData.push({
			json: {
				id: documentId,
				pageContent: textContent,
				metadata,
			},
			pairedItem: { item: itemIndex },
		});
	}

	if (documentsToUpdate.length === 0) {
		return resultData;
	}

	// Get the vector store client to check dimensions BEFORE deleting
	const vectorStore = await args.getVectorStoreClient(context, undefined, embeddings, 0);

	// Get batch size from parameters (same as insert operation)
	const embeddingBatchSize =
		(context.getNodeParameter('embeddingBatchSize', 0, 200) as number) ?? 200;

	// Embed all documents upfront in batches to:
	// 1. Check dimensions match before deleting anything
	// 2. Avoid embedding the same document twice
	const allVectors: number[][] = [];
	for (let i = 0; i < documentsToUpdate.length; i += embeddingBatchSize) {
		if (context.getExecutionCancelSignal()?.aborted) {
			break;
		}
		const batch = documentsToUpdate.slice(i, i + embeddingBatchSize);
		const texts = batch.map((d) => d.textContent);
		const vectors = await embeddings.embedDocuments(texts);
		allVectors.push(...vectors);
	}

	if (allVectors.length === 0) {
		return resultData;
	}

	const embeddingDimension = allVectors[0].length;

	// Check if index exists and get its dimension
	try {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const indexInfo = await (vectorStore as any).client?.indices?.getMapping({
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			index: (vectorStore as any).indexName,
		});

		if (indexInfo?.body) {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const indexName = (vectorStore as any).indexName;
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const vectorFieldName = (vectorStore as any).vectorFieldName || 'embedding';
			const mapping = indexInfo.body[indexName]?.mappings?.properties?.[vectorFieldName];

			if (mapping?.dimension && mapping.dimension !== embeddingDimension) {
				throw new NodeOperationError(
					context.getNode(),
					`Embedding dimension mismatch: index "${indexName}" expects ${mapping.dimension} dimensions, but your embedding model produces ${embeddingDimension} dimensions. Delete the index and recreate it, or use a compatible embedding model.`,
				);
			}
		}
	} catch (error) {
		// If error is our dimension mismatch error, rethrow it
		if (error instanceof NodeOperationError) {
			throw error;
		}
		// Otherwise index might not exist yet, which is fine - it will be created
	}

	// Delete all existing documents first if deleteDocument is provided
	if (args.deleteDocument) {
		for (const { id, itemIndex } of documentsToUpdate) {
			if (context.getExecutionCancelSignal()?.aborted) {
				break;
			}
			await args.deleteDocument(context, itemIndex, id);
		}
	}

	try {
		// Insert documents in batches using pre-computed vectors
		// We call addVectors directly instead of addDocuments because
		// LangChain's OpenSearchVectorStore.addDocuments doesn't pass the ids option through
		for (let i = 0; i < documentsToUpdate.length; i += embeddingBatchSize) {
			if (context.getExecutionCancelSignal()?.aborted) {
				break;
			}

			const batch = documentsToUpdate.slice(i, i + embeddingBatchSize);
			const documents = batch.map((d) => d.document);
			const ids = batch.map((d) => d.id);
			const vectors = allVectors.slice(i, i + embeddingBatchSize);

			// Call addVectors directly with IDs (bypasses broken addDocuments)
			try {
				await vectorStore.addVectors(vectors, documents, { ids });
			} catch (insertError) {
				throw new NodeOperationError(
					context.getNode(),
					`Failed to insert documents: ${insertError.message}`,
					{ cause: insertError },
				);
			}
		}

		// Log the AI event for analytics
		logAiEvent(context, 'ai-vector-store-updated');
	} finally {
		// Release the vector store client if a release method was provided
		args.releaseVectorStoreClient?.(vectorStore);
	}

	return resultData;
}
