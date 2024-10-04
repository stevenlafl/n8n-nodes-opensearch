// from @n8n/n8n-nodes-langchain:1.48.0
import type { Document } from '@langchain/core/documents';
import type { INodeExecutionData } from 'n8n-workflow';
import { N8nJsonLoader } from '../../../utils/N8nJsonLoader';
import { N8nBinaryLoader } from '../../../utils/N8nBinaryLoader';

export async function processDocuments(
	documentInput: N8nJsonLoader | N8nBinaryLoader | Array<Document<Record<string, unknown>>>,
	inputItems: INodeExecutionData[],
) {
	let processedDocuments: Document[];

	if (documentInput !== undefined
		&& documentInput.constructor !== undefined
		&& (documentInput.constructor.name === 'N8nJsonLoader' || documentInput.constructor.name === 'N8nBinaryLoader')
	) {
		processedDocuments = await (documentInput as (N8nBinaryLoader | N8nJsonLoader)).processAll(inputItems);
	} else {
		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		processedDocuments = documentInput as any;
	}

	const serializedDocuments = processedDocuments.map(({ metadata, pageContent }) => ({
		json: { metadata, pageContent },
	}));

	return {
		processedDocuments,
		serializedDocuments,
	};
}
export async function processDocument(
	documentInput: N8nJsonLoader | N8nBinaryLoader | Array<Document<Record<string, unknown>>>,
	inputItem: INodeExecutionData,
	itemIndex: number,
) {
	let processedDocuments: Document[];

	if (documentInput !== undefined
		&& documentInput.constructor !== undefined
		&& (documentInput.constructor.name === 'N8nJsonLoader' || documentInput.constructor.name === 'N8nBinaryLoader')
	) {
		processedDocuments = await (documentInput as (N8nBinaryLoader | N8nJsonLoader)).processItem(inputItem, itemIndex);
	} else {
		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		processedDocuments = documentInput as any;
	}

	const serializedDocuments = processedDocuments.map(({ metadata, pageContent }) => ({
		json: { metadata, pageContent },
		pairedItem: {
			item: itemIndex,
		},
	}));

	return {
		processedDocuments,
		serializedDocuments,
	};
}
