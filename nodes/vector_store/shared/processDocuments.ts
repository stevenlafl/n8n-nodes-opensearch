/* eslint-disable @n8n/community-nodes/no-restricted-imports, @typescript-eslint/ban-ts-comment */
// @ts-nocheck
import type { Document } from '@langchain/core/documents';
import type { INodeExecutionData } from 'n8n-workflow';

import { N8nBinaryLoader } from '../../../utils/N8nBinaryLoader';
import { N8nJsonLoader } from '../../../utils/N8nJsonLoader';

// Type guard using duck typing - checks if the object has processAll/processItem methods
// This is needed because n8n's built-in document loaders return their own N8nJsonLoader/N8nBinaryLoader
// instances which are different classes from our local ones, causing instanceof to fail
function isDocumentLoader(
	input: unknown,
): input is { processAll: (items: INodeExecutionData[]) => Promise<Document[]>; processItem: (item: INodeExecutionData, itemIndex: number) => Promise<Document[]> } {
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
	documentInput: N8nJsonLoader | N8nBinaryLoader | Array<Document<Record<string, unknown>>>,
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
	documentInput: N8nJsonLoader | N8nBinaryLoader | Array<Document<Record<string, unknown>>>,
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
