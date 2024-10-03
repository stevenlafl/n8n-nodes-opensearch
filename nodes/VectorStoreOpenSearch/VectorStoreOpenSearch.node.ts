import { OpenSearchVectorStore } from '@langchain/community/vectorstores/opensearch';
import {
	Client as OpenSearchClient,
	ClientOptions as OpenSearchClientOptions,
} from '@opensearch-project/opensearch';
import { createVectorStoreNode } from '@n8n/n8n-nodes-langchain/dist/nodes/vector_store/shared/createVectorStoreNode';
import type { INodeProperties } from 'n8n-workflow';
import { metadataFilterField } from '@n8n/n8n-nodes-langchain/dist/utils/sharedFields';
import https from 'https';

const sharedFields: INodeProperties[] = [
	{
		displayName: 'Index Name',
		name: 'indexName',
		type: 'string',
		default: 'vectors',
		description: 'The OpenSearch index name to store the vectors in.',
	},
];

const columnNamesField: INodeProperties = {
	displayName: 'Column Names',
	name: 'columnNames',
	type: 'fixedCollection',
	description: 'The names of the columns in the PGVector table',
	default: {
		values: {
			idColumnName: 'id',
			vectorColumnName: 'embedding',
			contentColumnName: 'text',
			metadataColumnName: 'metadata',
		},
	},
	typeOptions: {},
	placeholder: 'Set Column Names',
	options: [
		{
			name: 'values',
			displayName: 'Column Name Settings',
			values: [
				{
					displayName: 'ID Column Name',
					name: 'idColumnName',
					type: 'string',
					default: 'id',
					required: true,
				},
				{
					displayName: 'Vector Column Name',
					name: 'vectorColumnName',
					type: 'string',
					default: 'embedding',
					required: true,
				},
				{
					displayName: 'Content Column Name',
					name: 'contentColumnName',
					type: 'string',
					default: 'text',
					required: true,
				},
				{
					displayName: 'Metadata Column Name',
					name: 'metadataColumnName',
					type: 'string',
					default: 'metadata',
					required: true,
				},
			],
		},
	],
};

const distanceStrategyField: INodeProperties = {
	displayName: 'Distance Strategy',
	name: 'distanceStrategy',
	type: 'options',
	default: 'cosine',
	description: 'The method to calculate the distance between two vectors',
	options: [
		{
			name: 'Cosine',
			value: 'cosine',
		},
		{
			name: 'Inner Product',
			value: 'innerProduct',
		},
		{
			name: 'Euclidean',
			value: 'euclidean',
		},
	],
};

const insertFields: INodeProperties[] = [
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Option',
		default: {},
		options: [columnNamesField],
	},
];

const retrieveFields: INodeProperties[] = [
	{
		displayName: 'Options',
		name: 'options',
		type: 'collection',
		placeholder: 'Add Option',
		default: {},
		options: [distanceStrategyField, columnNamesField, metadataFilterField],
	},
];

const sslAgent = new https.Agent({
	rejectUnauthorized: false, // Disable SSL verification
});

export const VectorStoreOpenSearch = createVectorStoreNode({
	meta: {
		description: 'Work with your data in OpenSearch for vector-based search',
		icon: 'file:opensearch.svg',
		displayName: 'OpenSearch Vector Store',
		name: 'vectorStoreOpenSearch',
		credentials: [{ name: 'openSearchApi', required: true }],
		operationModes: ['load', 'insert', 'retrieve'],
		docsUrl:
			'https://docs.n8n.io/integrations/builtin/cluster-nodes/root-nodes/n8n-nodes-langchain.vectorstoreopensearch/',
	},
	sharedFields,
	insertFields,
	loadFields: retrieveFields,
	retrieveFields,
	async getVectorStoreClient(context, filter, embeddings, itemIndex) {
		const indexName = context.getNodeParameter('indexName', itemIndex, '', {
			extractValue: true,
		}) as string;

		const credentials = await context.getCredentials('openSearchApi');

		let clientOptions: OpenSearchClientOptions = {
			node: String(credentials.baseUrl),
			auth: {
				username: String(credentials.username),
				password: String(credentials.password),
			},
		};

		if (credentials.ignoreSSLIssues) {
			clientOptions.ssl = { rejectUnauthorized: false };
			clientOptions.agent = sslAgent;
		}

		const osClient = new OpenSearchClient(clientOptions);

		const config = {
			client: osClient,
			indexName,
		};

		return new OpenSearchVectorStore(embeddings, config);
	},
	async populateVectorStore(context, embeddings, documents, itemIndex) {
		const indexName = context.getNodeParameter('indexName', itemIndex, '', {
			extractValue: true,
		}) as string;

		const credentials = await context.getCredentials('openSearchApi');

		let clientOptions: OpenSearchClientOptions = {
			node: String(credentials.baseUrl),
			auth: {
				username: String(credentials.username),
				password: String(credentials.password),
			},
		};

		if (credentials.ignoreSSLIssues) {
			clientOptions.ssl = { rejectUnauthorized: false };
			clientOptions.agent = sslAgent;
		}

		const osClient = new OpenSearchClient(clientOptions);

		const config: any = {
			client: osClient,
			indexName,
		};

		await OpenSearchVectorStore.fromDocuments(documents, embeddings, config);
	},
});
