import { OpenSearchClientArgs, OpenSearchVectorStore } from '@langchain/community/vectorstores/opensearch';
import {
	Client as OpenSearchClient,
	type ClientOptions as OpenSearchClientOptions,
} from '@opensearch-project/opensearch';
import { createVectorStoreNode } from '../shared/createVectorStoreNode';
import type { INodeProperties } from 'n8n-workflow';
import { metadataFilterField } from '../../../utils/sharedFields';
import https from 'node:https';

type FieldOptions = {
	vectorFieldName: string;
	textFieldName: string;
	metadataFieldName: string;
};

const sharedFields: INodeProperties[] = [
	{
		displayName: 'Index Name',
		name: 'indexName',
		type: 'string',
		default: 'vectors',
		description: 'The OpenSearch index name to store the vectors in',
	},
];

const fieldNamesField: INodeProperties = {
	displayName: 'Field Names',
	name: 'fieldNames',
	type: 'fixedCollection',
	description: 'The names of the fields in the OpenSearch index',
	default: {
		values: {
			vectorFieldName: 'embedding',
			textFieldName: 'text',
			metadataFieldName: 'metadata',
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
					default: 'embedding',
					required: true,
				},
				{
					displayName: 'Content Field Name',
					name: 'contentFieldName',
					type: 'string',
					default: 'text',
					required: true,
				},
				{
					displayName: 'Metadata Field Name',
					name: 'metadataFieldName',
					type: 'string',
					default: 'metadata',
					required: true,
				},
			],
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

		const clientOptions: OpenSearchClientOptions = {
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

		const fieldNames = context.getNodeParameter('options.fieldNames.values', 0, {
			vectorFieldName: 'embedding',
			contentFieldName: 'text',
			metadataFieldName: 'metadata',
		}) as FieldOptions;

		const config: OpenSearchClientArgs = {
			client: osClient,
			indexName,
			...fieldNames
		};

		console.log('hello');

		return new OpenSearchVectorStore(embeddings, config);
	},
	async populateVectorStore(context, embeddings, documents, itemIndex) {
		const indexName = context.getNodeParameter('indexName', itemIndex, '', {
			extractValue: true,
		}) as string;

		const credentials = await context.getCredentials('openSearchApi');

		const clientOptions: OpenSearchClientOptions = {
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

		const fieldNames = context.getNodeParameter('options.fieldNames.values', 0, {
			vectorFieldName: 'embedding',
			contentFieldName: 'text',
			metadataFieldName: 'metadata',
		}) as FieldOptions;

		const config: OpenSearchClientArgs = {
			client: osClient,
			indexName,
			...fieldNames
		};

		console.log("hello", documents);

		await OpenSearchVectorStore.fromDocuments(documents, embeddings, config);
	},
});
