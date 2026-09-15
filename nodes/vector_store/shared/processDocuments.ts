/* eslint-disable @n8n/community-nodes/no-restricted-imports, @typescript-eslint/ban-ts-comment */
// @ts-nocheck
import type { Document } from '@langchain/core/documents';
import type { INodeExecutionData } from 'n8n-workflow';

// Shape of the N8nJsonLoader/N8nBinaryLoader instances that n8n's own document loader nodes
// supply on the AiDocument input. Those classes live inside n8n, so only the interface is described here.
export interface DocumentLoader {
	processAll: (items: INodeExecutionData[]) => Promise<Document[]>;
	processItem: (item: INodeExecutionData, itemIndex: number) => Promise<Document[]>;
}

// Type guard using duck typing - checks if the object has processAll/processItem methods
function isDocumentLoader(input: unknown): input is DocumentLoader {
	return (
		input !== null &&
		typeof input === 'object' &&
		'processAll' in input &&
		typeof (input as Record<string, unknown>).processAll === 'function' &&
		'processItem' in input &&
		typeof (input as Record<string, unknown>).processItem === 'function'
	);
}

export async function processDocuments(
	documentInput: DocumentLoader | Array<Document<Record<string, unknown>>>,
	inputItems: INodeExecutionData[],
) {
	let processedDocuments: Document[];

	if (isDocumentLoader(documentInput)) {
		processedDocuments = await documentInput.processAll(inputItems);
	} else if (Array.isArray(documentInput)) {
		processedDocuments = documentInput;
	} else {
		throw new Error(`Invalid document input type: expected document loader or array of documents`);
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
	documentInput: DocumentLoader | Array<Document<Record<string, unknown>>>,
	inputItem: INodeExecutionData,
	itemIndex: number,
) {
	let processedDocuments: Document[];

	if (isDocumentLoader(documentInput)) {
		processedDocuments = await documentInput.processItem(inputItem, itemIndex);
	} else if (Array.isArray(documentInput)) {
		processedDocuments = documentInput;
	} else {
		throw new Error(`Invalid document input type: expected document loader or array of documents`);
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
