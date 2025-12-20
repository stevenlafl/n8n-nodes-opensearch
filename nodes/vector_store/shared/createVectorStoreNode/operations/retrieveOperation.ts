/* eslint-disable @n8n/community-nodes/no-restricted-imports, @typescript-eslint/ban-ts-comment */
// @ts-nocheck
import type { Embeddings } from '@langchain/core/embeddings';
import type { BaseDocumentCompressor } from '@langchain/core/retrievers/document_compressors';
import type { VectorStore } from '@langchain/core/vectorstores';
import { NodeConnectionTypes, type ISupplyDataFunctions, type SupplyData } from 'n8n-workflow';

import { getMetadataFiltersValues } from '../../../../../utils/helpers';
import { logWrapper } from '../../../../../utils/logWrapper';

import type { VectorStoreNodeConstructorArgs } from '../types';

/**
 * Handles the 'retrieve' operation mode
 * Returns the vector store to be used with AI nodes
 */
export async function handleRetrieveOperation<T extends VectorStore = VectorStore>(
	context: ISupplyDataFunctions,
	args: VectorStoreNodeConstructorArgs<T>,
	embeddings: Embeddings,
	itemIndex: number,
): Promise<SupplyData> {
	// Get metadata filters
	const filter = getMetadataFiltersValues(context, itemIndex);
	const useReranker = context.getNodeParameter('useReranker', itemIndex, false) as boolean;

	// Get the vector store client
	const vectorStore = await args.getVectorStoreClient(context, filter, embeddings, itemIndex);
	let response: VectorStore | { reranker: BaseDocumentCompressor; vectorStore: VectorStore } =
		vectorStore;

	// Wrap the vector store with logging
	const wrappedVectorStore = logWrapper(vectorStore, context);

	if (useReranker) {
		const reranker = (await context.getInputConnectionData(
			NodeConnectionTypes.AiReranker,
			0,
		)) as BaseDocumentCompressor;

		// Return reranker and vector store with log wrapper
		response = {
			reranker,
			vectorStore: wrappedVectorStore,
		};
	} else {
		// Return the vector store with logging wrapper
		// IMPORTANT: Also add vectorStore and reranker properties for compatibility
		// This handles the case where n8n's RetrieverVectorStore's `instanceof VectorStore` check fails
		// (due to different package versions in custom nodes) and falls back to accessing .vectorStore
		// The reranker is a passthrough that just returns documents unchanged
		const passthroughReranker = {
			compressDocuments: async (documents: Document[]) => documents,
		} as BaseDocumentCompressor;

		response = Object.assign(wrappedVectorStore, {
			vectorStore: wrappedVectorStore,
			reranker: passthroughReranker,
		});
	}

	return {
		response,
		closeFunction: async () => {
			// Release the vector store client if a release method was provided
			args.releaseVectorStoreClient?.(vectorStore);
		},
	};
}
