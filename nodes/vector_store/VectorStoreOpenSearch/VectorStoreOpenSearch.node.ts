import { OpenSearchClientArgs, OpenSearchVectorStore } from '../../../utils/OpenSearchVectorStore';
import {
	Client as OpenSearchClient,
	type ClientOptions as OpenSearchClientOptions,
} from '@opensearch-project/opensearch';
import { createVectorStoreNode } from '../shared/createVectorStoreNode/createVectorStoreNode';
import type { ICredentialDataDecryptedObject, ILoadOptionsFunctions, INodeProperties } from 'n8n-workflow';
import { metadataFilterField } from '../../../utils/sharedFields';
import https from 'node:https';

type FieldOptions = {
	vectorFieldName: string;
	textFieldName: string;
	metadataFieldName: string;
};

type VectorSearchEngine = 'lucene' | 'faiss' | 'nmslib';

// Default field names for vector store
const DEFAULT_VECTOR_FIELD = 'embedding';
const DEFAULT_TEXT_FIELD = 'text';
const DEFAULT_METADATA_FIELD = 'metadata';

const sslAgent = new https.Agent({
	rejectUnauthorized: false,
});

/**
 * Creates an OpenSearch client with proper SSL configuration
 */
function createOpenSearchClient(credentials: ICredentialDataDecryptedObject): OpenSearchClient {
	const baseUrl = String(credentials.baseUrl).replace(/\/$/, '');
	const isHttps = baseUrl.toLowerCase().startsWith('https://');

	const clientOptions: OpenSearchClientOptions = {
		node: baseUrl,
		auth: {
			username: String(credentials.username),
			password: String(credentials.password),
		},
	};

	if (credentials.ignoreSSLIssues && isHttps) {
		clientOptions.ssl = { rejectUnauthorized: false };
		clientOptions.agent = sslAgent;
	}

	return new OpenSearchClient(clientOptions);
}

/**
 * Fetches available OpenSearch indices for the RLC dropdown
 */
async function openSearchIndexSearch(this: ILoadOptionsFunctions) {
	const credentials = await this.getCredentials('openSearchApi');
	const client = createOpenSearchClient(credentials);

	try {
		const response = await client.cat.indices({ format: 'json' });
		const indices = response.body as Array<{ index: string }>;

		const results = indices
			.filter((idx) => !idx.index.startsWith('.')) // Filter out system indices
			.map((idx) => ({
				name: idx.index,
				value: idx.index,
			}));

		return { results };
	} catch (error) {
		// Return empty results if we can't fetch indices
		return { results: [] };
	}
}

/**
 * Resource Locator Component for OpenSearch index selection
 */
const openSearchIndexRLC: INodeProperties = {
	displayName: 'Index Name',
	name: 'indexName',
	type: 'resourceLocator',
	default: { mode: 'list', value: '' },
	required: true,
	description: 'The OpenSearch index name to store the vectors in',
	modes: [
		{
			displayName: 'From List',
			name: 'list',
			type: 'list',
			typeOptions: {
				searchListMethod: 'openSearchIndexSearch',
			},
		},
		{
			displayName: 'Name',
			name: 'id',
			type: 'string',
			placeholder: 'e.g. vectors',
		},
	],
};

const sharedFields: INodeProperties[] = [openSearchIndexRLC];

const fieldNamesField: INodeProperties = {
	displayName: 'Field Names',
	name: 'fieldNames',
	type: 'fixedCollection',
	description: 'The names of the fields in the OpenSearch index',
	default: {
		values: {
			vectorFieldName: DEFAULT_VECTOR_FIELD,
			textFieldName: DEFAULT_TEXT_FIELD,
			metadataFieldName: DEFAULT_METADATA_FIELD,
		},
	},
	typeOptions: {},
	placeholder: 'Set Field Names',
	options: [
		{
			name: 'values',
			displayName: 'Field Name Settings',
			values: [
				{
					displayName: 'Vector Field Name',
					name: 'vectorFieldName',
					type: 'string',
					default: DEFAULT_VECTOR_FIELD,
					required: true,
				},
				{
					displayName: 'Text Field Name',
					name: 'textFieldName',
					type: 'string',
					default: DEFAULT_TEXT_FIELD,
					required: true,
				},
				{
					displayName: 'Metadata Field Name',
					name: 'metadataFieldName',
					type: 'string',
					default: DEFAULT_METADATA_FIELD,
					required: true,
				},
			],
		},
	],
};

const engineField: INodeProperties = {
	displayName: 'Vector Search Engine',
	name: 'engine',
	type: 'options',
	default: 'lucene',
	description: 'The k-NN engine to use for vector search. Use "lucene" for OpenSearch 3.x+, "nmslib" for older versions.',
	options: [
		{
			name: 'Lucene (Recommended for OpenSearch 3.x+)',
			value: 'lucene',
		},
		{
			name: 'Faiss',
			value: 'faiss',
		},
		{
			name: 'NMSLIB (Legacy, OpenSearch < 3.0)',
			value: 'nmslib',
		},
	],
};

const insertFields: INodeProperties[] = [
	engineField,
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Option',
		default: {},
		options: [
			{
				displayName: 'Clear Index',
				name: 'clearIndex',
				type: 'boolean',
				default: false,
				description: 'Whether to delete the index before inserting new data. The index will be recreated with the new documents.',
			},
			fieldNamesField,
		],
	},
];

const updateFields: INodeProperties[] = [
	engineField,
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Option',
		default: {},
		options: [fieldNamesField],
	},
];

const retrieveFields: INodeProperties[] = [
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Option',
		default: {},
		options: [fieldNamesField, metadataFilterField],
	},
];

export class VectorStoreOpenSearch extends createVectorStoreNode({
	meta: {
		description: 'Work with your data in OpenSearch for vector-based search',
		icon: 'file:opensearch.svg',
		displayName: 'OpenSearch Vector Store',
		name: 'vectorStoreOpenSearch',
		credentials: [{ name: 'openSearchApi', required: true }],
		docsUrl:
			'https://docs.n8n.io/integrations/builtin/cluster-nodes/root-nodes/n8n-nodes-langchain.vectorstoreopensearch/',
		operationModes: ['load', 'insert', 'retrieve', 'update', 'retrieve-as-tool'],
		alias: ['opensearch', 'vector', 'embedding', 'search'],
	},
	methods: { listSearch: { openSearchIndexSearch } },
	sharedFields,
	insertFields,
	updateFields,
	loadFields: retrieveFields,
	retrieveFields,
	async deleteDocument(context, itemIndex, documentId) {
		const indexName = context.getNodeParameter('indexName', itemIndex, '', {
			extractValue: true,
		}) as string;

		const credentials = await context.getCredentials('openSearchApi');
		const osClient = createOpenSearchClient(credentials);

		// Delete the document by ID - ignore if not found
		try {
			await osClient.delete({
				index: indexName,
				id: documentId,
			});
		} catch (error) {
			// Ignore 404 errors (document not found) - this allows update to work as upsert
			if (error?.meta?.statusCode !== 404) {
				throw error;
			}
		}
	},
	async getVectorStoreClient(context, filter, embeddings, itemIndex) {
		const indexName = context.getNodeParameter('indexName', itemIndex, '', {
			extractValue: true,
		}) as string;

		const credentials = await context.getCredentials('openSearchApi');
		const osClient = createOpenSearchClient(credentials);

		const fieldNames = context.getNodeParameter('options.fieldNames.values', 0, {
			vectorFieldName: DEFAULT_VECTOR_FIELD,
			textFieldName: DEFAULT_TEXT_FIELD,
			metadataFieldName: DEFAULT_METADATA_FIELD,
		}) as FieldOptions;

		const engine = context.getNodeParameter('engine', 0, 'lucene') as VectorSearchEngine;

		const config: OpenSearchClientArgs = {
			client: osClient,
			indexName,
			...fieldNames,
			vectorSearchOptions: {
				// Cast needed: LangChain types are outdated, but runtime supports lucene/faiss/nmslib
				engine: engine as 'nmslib' | 'hnsw',
			},
		};

		return new OpenSearchVectorStore(embeddings, config);
	},
	async populateVectorStore(context, embeddings, documents, itemIndex) {
		const indexName = context.getNodeParameter('indexName', itemIndex, '', {
			extractValue: true,
		}) as string;

		const options = context.getNodeParameter('options', itemIndex, {}) as {
			clearIndex?: boolean;
			fieldNames?: { values: FieldOptions };
		};

		const credentials = await context.getCredentials('openSearchApi');
		const osClient = createOpenSearchClient(credentials);

		// Delete the index if requested (will be recreated by fromDocuments)
		if (options.clearIndex) {
			try {
				await osClient.indices.delete({ index: indexName });
				context.logger.info(`Deleted index: ${indexName}`);
			} catch (error) {
				// Index might not exist yet, which is fine - it will be created
				context.logger.debug(`Could not delete index ${indexName} (may not exist yet)`);
			}
		}

		const fieldNames = options.fieldNames?.values ?? {
			vectorFieldName: DEFAULT_VECTOR_FIELD,
			textFieldName: DEFAULT_TEXT_FIELD,
			metadataFieldName: DEFAULT_METADATA_FIELD,
		};

		const engine = context.getNodeParameter('engine', 0, 'lucene') as VectorSearchEngine;

		const config: OpenSearchClientArgs = {
			client: osClient,
			indexName,
			...fieldNames,
			vectorSearchOptions: {
				// Cast needed: LangChain types are outdated, but runtime supports lucene/faiss/nmslib
				engine: engine as 'nmslib' | 'hnsw',
			},
		};

		await OpenSearchVectorStore.fromDocuments(documents, embeddings, config);
	},
}) {}
