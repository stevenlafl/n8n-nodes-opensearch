import { Client, RequestParams, errors } from '@opensearch-project/opensearch';
import type { EmbeddingsInterface } from '@langchain/core/embeddings';
import { VectorStore } from '@langchain/core/vectorstores';
import { Document } from '@langchain/core/documents';

type OpenSearchEngine = 'nmslib' | 'hnsw' | 'lucene' | 'faiss';
type OpenSearchSpaceType = 'l2' | 'cosinesimil' | 'ip';

/**
 * Interface defining the options for vector search in OpenSearch.
 */
interface VectorSearchOptions {
	readonly engine?: OpenSearchEngine;
	readonly spaceType?: OpenSearchSpaceType;
	readonly m?: number;
	readonly efConstruction?: number;
	readonly efSearch?: number;
	readonly numberOfShards?: number;
	readonly numberOfReplicas?: number;
}

/**
 * Interface defining the arguments required to create an instance of the
 * OpenSearchVectorStore class.
 */
export interface OpenSearchClientArgs {
	readonly client: Client;
	readonly vectorFieldName?: string;
	readonly textFieldName?: string;
	readonly metadataFieldName?: string;
	readonly service?: 'es' | 'aoss';
	readonly indexName?: string;
	readonly vectorSearchOptions?: VectorSearchOptions;
}

/**
 * Type alias for OpenSearch filter objects.
 */
type OpenSearchFilter = {
	[key: string]: FilterTypeValue | (string | number)[] | string | number;
};

/**
 * FilterTypeValue for OpenSearch queries.
 */
interface FilterTypeValue {
	exists?: boolean;
	fuzzy?: string;
	ids?: string[];
	prefix?: string;
	gte?: number;
	gt?: number;
	lte?: number;
	lt?: number;
	regexp?: string;
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	terms_set?: Record<string, any>;
	wildcard?: string;
}

/**
 * Generate a UUID v4
 */
function generateUUID(): string {
	return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
		const r = (Math.random() * 16) | 0;
		const v = c === 'x' ? r : (r & 0x3) | 0x8;
		return v.toString(16);
	});
}

/**
 * Custom OpenSearchVectorStore implementation that extends @langchain/core VectorStore.
 * This avoids dependency on @langchain/community which causes module resolution issues.
 */
export class OpenSearchVectorStore extends VectorStore {
	declare FilterType: OpenSearchFilter;

	private readonly client: Client;

	private readonly indexName: string;

	private readonly isAoss: boolean;

	private readonly engine: OpenSearchEngine;

	private readonly spaceType: OpenSearchSpaceType;

	private readonly efConstruction: number;

	private readonly efSearch: number;

	private readonly numberOfShards: number;

	private readonly numberOfReplicas: number;

	private readonly m: number;

	private readonly vectorFieldName: string;

	private readonly textFieldName: string;

	private readonly metadataFieldName: string;

	_vectorstoreType(): string {
		return 'opensearch';
	}

	constructor(embeddings: EmbeddingsInterface, args: OpenSearchClientArgs) {
		super(embeddings, args);

		this.spaceType = args.vectorSearchOptions?.spaceType ?? 'l2';
		this.engine = args.vectorSearchOptions?.engine ?? 'nmslib';
		this.m = args.vectorSearchOptions?.m ?? 16;
		this.efConstruction = args.vectorSearchOptions?.efConstruction ?? 512;
		this.efSearch = args.vectorSearchOptions?.efSearch ?? 512;
		this.numberOfShards = args.vectorSearchOptions?.numberOfShards ?? 5;
		this.numberOfReplicas = args.vectorSearchOptions?.numberOfReplicas ?? 1;
		this.vectorFieldName = args.vectorFieldName ?? 'embedding';
		this.textFieldName = args.textFieldName ?? 'text';
		this.metadataFieldName = args.metadataFieldName ?? 'metadata';

		this.client = args.client;
		this.indexName = args.indexName ?? 'documents';
		this.isAoss = (args.service ?? 'es') === 'aoss';
	}

	/**
	 * Add documents to the OpenSearch index.
	 */
	async addDocuments(documents: Document[]): Promise<void> {
		const texts = documents.map(({ pageContent }) => pageContent);
		return this.addVectors(await this.embeddings.embedDocuments(texts), documents);
	}

	/**
	 * Add vectors to the OpenSearch index.
	 */
	async addVectors(
		vectors: number[][],
		documents: Document[],
		options?: { ids?: string[] },
	): Promise<void> {
		await this.ensureIndexExists(
			vectors[0].length,
			this.engine,
			this.spaceType,
			this.efSearch,
			this.efConstruction,
			this.numberOfShards,
			this.numberOfReplicas,
			this.m,
		);

		const documentIds = options?.ids ?? Array.from({ length: vectors.length }, () => generateUUID());

		const operations = vectors.flatMap((embedding, idx) => {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const document: Record<string, any>[] = [
				{
					index: {
						_index: this.indexName,
						_id: documentIds[idx],
					},
				},
				{
					[this.vectorFieldName]: embedding,
					[this.textFieldName]: documents[idx].pageContent,
					[this.metadataFieldName]: documents[idx].metadata,
				},
			];

			// aoss does not support document id
			if (this.isAoss) {
				delete document[0].index?._id;
			}

			return document;
		});

		await this.client.bulk({ body: operations });

		// aoss does not support refresh
		if (!this.isAoss) {
			await this.client.indices.refresh({ index: this.indexName });
		}
	}

	/**
	 * Perform similarity search using a query vector.
	 */
	async similaritySearchVectorWithScore(
		query: number[],
		k: number,
		filter?: OpenSearchFilter | undefined,
	): Promise<[Document, number][]> {
		const search: RequestParams.Search = {
			index: this.indexName,
			body: {
				query: {
					bool: {
						filter: { bool: this.buildMetadataTerms(filter) },
						must: [
							{
								knn: {
									[this.vectorFieldName]: { vector: query, k },
								},
							},
						],
					},
				},
				size: k,
			},
		};

		const { body } = await this.client.search(search);

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		return body.hits.hits.map((hit: any) => [
			new Document({
				pageContent: hit._source[this.textFieldName],
				metadata: hit._source[this.metadataFieldName],
				id: hit._id,
			}),
			hit._score,
		]);
	}

	/**
	 * Create a new OpenSearchVectorStore from texts.
	 */
	static fromTexts(
		texts: string[],
		metadatas: object[] | object,
		embeddings: EmbeddingsInterface,
		args: OpenSearchClientArgs,
	): Promise<OpenSearchVectorStore> {
		const documents = texts.map((text, idx) => {
			const metadata = Array.isArray(metadatas) ? metadatas[idx] : metadatas;
			return new Document({ pageContent: text, metadata });
		});

		return OpenSearchVectorStore.fromDocuments(documents, embeddings, args);
	}

	/**
	 * Create a new OpenSearchVectorStore from documents.
	 */
	static async fromDocuments(
		docs: Document[],
		embeddings: EmbeddingsInterface,
		dbConfig: OpenSearchClientArgs,
	): Promise<OpenSearchVectorStore> {
		const store = new OpenSearchVectorStore(embeddings, dbConfig);
		await store.addDocuments(docs).then(() => store);
		return store;
	}

	/**
	 * Create a new OpenSearchVectorStore from an existing index.
	 */
	static async fromExistingIndex(
		embeddings: EmbeddingsInterface,
		dbConfig: OpenSearchClientArgs,
	): Promise<OpenSearchVectorStore> {
		const store = new OpenSearchVectorStore(embeddings, dbConfig);
		await store.client.cat.indices({ index: store.indexName });
		return store;
	}

	private async ensureIndexExists(
		dimension: number,
		engine: OpenSearchEngine = 'nmslib',
		spaceType: OpenSearchSpaceType = 'l2',
		efSearch = 512,
		efConstruction = 512,
		numberOfShards = 5,
		numberOfReplicas = 1,
		m = 16,
	): Promise<void> {
		const body = {
			settings: {
				index: {
					number_of_shards: numberOfShards,
					number_of_replicas: numberOfReplicas,
					knn: true,
					'knn.algo_param.ef_search': efSearch,
				},
			},
			mappings: {
				dynamic_templates: [
					{
						[`${this.metadataFieldName}.*`]: {
							match_mapping_type: 'string',
							mapping: { type: 'keyword' },
						},
					},
					{
						[`${this.metadataFieldName}.loc`]: {
							match_mapping_type: 'object',
							mapping: { type: 'object' },
						},
					},
				],
				properties: {
					[this.textFieldName]: { type: 'text' },
					[this.metadataFieldName]: { type: 'object' },
					[this.vectorFieldName]: {
						type: 'knn_vector',
						dimension,
						method: {
							name: 'hnsw',
							engine,
							space_type: spaceType,
							parameters: { ef_construction: efConstruction, m },
						},
					},
				},
			},
		};

		const indexExists = await this.doesIndexExist();
		if (indexExists) return;

		await this.client.indices.create({ index: this.indexName, body });
	}

	/**
	 * Build metadata terms for OpenSearch queries.
	 */
	buildMetadataTerms(filter: OpenSearchFilter | undefined): object {
		if (!filter) return {};
		const must = [];
		const must_not = [];

		for (const [key, value] of Object.entries(filter)) {
			const metadataKey = `${this.metadataFieldName}.${key}`;
			if (value) {
				if (typeof value === 'object' && !Array.isArray(value)) {
					if ('exists' in value) {
						if (value.exists) {
							must.push({ exists: { field: metadataKey } });
						} else {
							must_not.push({ exists: { field: metadataKey } });
						}
					} else if ('fuzzy' in value) {
						must.push({ fuzzy: { [metadataKey]: value.fuzzy } });
					} else if ('ids' in value) {
						must.push({ ids: { values: value.ids } });
					} else if ('prefix' in value) {
						must.push({ prefix: { [metadataKey]: value.prefix } });
					} else if ('gte' in value || 'gt' in value || 'lte' in value || 'lt' in value) {
						must.push({ range: { [metadataKey]: value } });
					} else if ('regexp' in value) {
						must.push({ regexp: { [metadataKey]: value.regexp } });
					} else if ('terms_set' in value) {
						must.push({ terms_set: { [metadataKey]: value.terms_set } });
					} else if ('wildcard' in value) {
						must.push({ wildcard: { [metadataKey]: value.wildcard } });
					}
				} else {
					const aggregatorKey = Array.isArray(value) ? 'terms' : 'term';
					must.push({ [aggregatorKey]: { [metadataKey]: value } });
				}
			}
		}
		return { must, must_not };
	}

	/**
	 * Check if the OpenSearch index exists.
	 */
	async doesIndexExist(): Promise<boolean> {
		try {
			await this.client.cat.indices({ index: this.indexName });
			return true;
		} catch (err: unknown) {
			// eslint-disable-next-line no-instanceof/no-instanceof
			if (err instanceof errors.ResponseError && err.statusCode === 404) {
				return false;
			}
			throw err;
		}
	}

	/**
	 * Delete the OpenSearch index if it exists.
	 */
	async deleteIfExists(): Promise<void> {
		const indexExists = await this.doesIndexExist();
		if (!indexExists) return;

		await this.client.indices.delete({ index: this.indexName });
	}
}
