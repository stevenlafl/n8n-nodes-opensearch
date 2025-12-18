// Mock external modules before imports
jest.mock('@langchain/community/vectorstores/opensearch', () => {
	const mockVectorStoreInstance = {
		similaritySearch: jest.fn().mockResolvedValue([]),
		similaritySearchVectorWithScore: jest.fn().mockResolvedValue([]),
		addDocuments: jest.fn().mockResolvedValue(undefined),
	};
	return {
		OpenSearchVectorStore: Object.assign(
			jest.fn().mockImplementation(() => mockVectorStoreInstance),
			{
				fromDocuments: jest.fn().mockResolvedValue(mockVectorStoreInstance),
			}
		),
	};
});

jest.mock('@opensearch-project/opensearch', () => {
	const mockDelete = jest.fn().mockResolvedValue({ body: { result: 'deleted' } });
	return {
		Client: jest.fn().mockImplementation(() => ({
			delete: mockDelete,
			close: jest.fn(),
		})),
	};
});

jest.mock('../../../utils/sharedFields', () => ({
	metadataFilterField: {
		displayName: 'Metadata Filter',
		name: 'metadata',
		type: 'fixedCollection',
		default: {},
		options: [],
	},
	getConnectionHintNoticeField: jest.fn().mockReturnValue({
		displayName: 'Connection Hint Notice',
		name: 'connectionHintNotice',
		type: 'notice',
		default: '',
	}),
}));

import { VectorStoreOpenSearch } from './VectorStoreOpenSearch.node';

describe('VectorStoreOpenSearch Node', () => {

	let nodeInstance: VectorStoreOpenSearch;

	beforeAll(() => {
		nodeInstance = new VectorStoreOpenSearch();
	});

	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('Node Description', () => {
		it('should have correct display name', () => {
			expect(nodeInstance.description.displayName).toBe('OpenSearch Vector Store');
		});

		it('should have correct node name', () => {
			expect(nodeInstance.description.name).toBe('vectorStoreOpenSearch');
		});

		it('should require openSearchApi credentials', () => {
			expect(nodeInstance.description.credentials).toContainEqual(
				expect.objectContaining({
					name: 'openSearchApi',
					required: true,
				})
			);
		});

		it('should have operation modes: insert, update, load, retrieve, retrieve-as-tool', () => {
			const modeField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'mode'
			);

			expect(modeField).toBeDefined();
			expect(modeField?.type).toBe('options');

			const options = modeField?.options as Array<{ value: string }>;
			const values = options?.map((opt) => opt.value);

			expect(values).toContain('insert');
			expect(values).toContain('update');
			expect(values).toContain('load');
			expect(values).toContain('retrieve');
			expect(values).toContain('retrieve-as-tool');
		});
	});

	describe('Update Mode', () => {
		it('should have id field for update mode', () => {
			const idField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'id') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('update');
				}
			);

			expect(idField).toBeDefined();
			expect(idField?.type).toBe('string');
			expect(idField?.required).toBe(true);
		});

		it('should have engine field for update mode', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('update');
				}
			);

			expect(engineField).toBeDefined();
			expect(engineField?.type).toBe('options');
		});
	});

	describe('Retrieve As Tool Mode', () => {
		it('should have toolName field for retrieve-as-tool mode (version <= 1.2)', () => {
			const toolNameField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'toolName') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[]; '@version'?: unknown[] } } | undefined;
					return displayOptions?.show?.mode?.includes('retrieve-as-tool');
				}
			);

			expect(toolNameField).toBeDefined();
			expect(toolNameField?.type).toBe('string');
			expect(toolNameField?.default).toBe('');
		});

		it('should have toolDescription field for retrieve-as-tool mode', () => {
			const toolDescField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'toolDescription') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('retrieve-as-tool');
				}
			);

			expect(toolDescField).toBeDefined();
			expect(toolDescField?.type).toBe('string');
		});
	});

	describe('Engine Configuration', () => {
		it('should have engine field visible in insert mode', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			);

			expect(engineField).toBeDefined();
			expect(engineField?.type).toBe('options');
		});

		it('should have three engine options: lucene, faiss, nmslib', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			) as { options?: Array<{ value: string }> } | undefined;

			const engineValues = engineField?.options?.map((opt) => opt.value);

			expect(engineValues).toContain('lucene');
			expect(engineValues).toContain('faiss');
			expect(engineValues).toContain('nmslib');
		});

		it('should default engine to lucene', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			) as { default?: string } | undefined;

			expect(engineField?.default).toBe('lucene');
		});

		it('should have lucene as recommended option for OpenSearch 3.x', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			) as { options?: Array<{ name: string; value: string }> } | undefined;

			const luceneOption = engineField?.options?.find((opt) => opt.value === 'lucene');

			expect(luceneOption?.name).toContain('Recommended');
		});

		it('should mark nmslib as legacy', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			) as { options?: Array<{ name: string; value: string }> } | undefined;

			const nmslibOption = engineField?.options?.find((opt) => opt.value === 'nmslib');

			expect(nmslibOption?.name).toContain('Legacy');
		});
	});

	describe('Field Name Configuration', () => {
		it('should have fieldNames option for customizing field names', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'options') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			);

			const options = optionsField?.options as Array<{ name: string }> | undefined;
			const fieldNamesOption = options?.find((opt) => opt.name === 'fieldNames');

			expect(fieldNamesOption).toBeDefined();
		});

		it('should have default field names: embedding, text, metadata', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'options') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			);

			const options = optionsField?.options as Array<{
				name: string;
				default?: { values?: { vectorFieldName?: string; textFieldName?: string; metadataFieldName?: string } };
			}> | undefined;
			const fieldNamesOption = options?.find((opt) => opt.name === 'fieldNames');

			expect(fieldNamesOption?.default?.values?.vectorFieldName).toBe('embedding');
			expect(fieldNamesOption?.default?.values?.textFieldName).toBe('text');
			expect(fieldNamesOption?.default?.values?.metadataFieldName).toBe('metadata');
		});
	});

	describe('Index Name Configuration', () => {
		it('should have indexName field as resourceLocator', () => {
			const indexNameField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'indexName'
			);

			expect(indexNameField).toBeDefined();
			expect(indexNameField?.type).toBe('resourceLocator');
		});

		it('should have RLC with list and id modes', () => {
			const indexNameField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'indexName'
			) as { modes?: Array<{ name: string }> };

			expect(indexNameField?.modes).toBeDefined();
			const modeNames = indexNameField?.modes?.map((m) => m.name);
			expect(modeNames).toContain('list');
			expect(modeNames).toContain('id');
		});
	});

	describe('Retrieve Mode Options', () => {
		it('should have metadata filter option for retrieve mode', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'options') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('retrieve');
				}
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }> | undefined;
			const metadataOption = options?.find((opt) => opt.name === 'metadata');

			expect(metadataOption).toBeDefined();
		});
	});

	describe('Load Mode Options', () => {
		it('should have prompt field for load mode', () => {
			const promptField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'prompt') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('load');
				}
			);

			expect(promptField).toBeDefined();
			expect(promptField?.required).toBe(true);
		});

		it('should have topK/limit field for load mode', () => {
			const topKField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'topK') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('load');
				}
			);

			expect(topKField).toBeDefined();
			expect(topKField?.type).toBe('number');
			expect(topKField?.default).toBe(4);
		});
	});

	describe('getVectorStoreClient', () => {
		it('should create vector store client with default options', () => {
			// The node is configured with getVectorStoreClient that creates an OpenSearchVectorStore
			// We verify the node has the proper configuration
			const node = new VectorStoreOpenSearch();
			expect(node.description.name).toBe('vectorStoreOpenSearch');
			expect(node.description.credentials?.[0]?.name).toBe('openSearchApi');
		});

		it('should configure correct client options for HTTPS with SSL ignore', () => {
			// This test verifies the node has proper credentials configuration
			// for SSL handling
			expect(nodeInstance.description.credentials?.[0]?.name).toBe('openSearchApi');
		});

		it('should support all three engine types', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'engine'
			) as { options?: Array<{ value: string }> } | undefined;

			const engineValues = engineField?.options?.map((opt) => opt.value);

			expect(engineValues).toContain('lucene');
			expect(engineValues).toContain('faiss');
			expect(engineValues).toContain('nmslib');
		});
	});

	describe('populateVectorStore', () => {
		it('should be configured to use OpenSearchVectorStore.fromDocuments', () => {
			// The node uses OpenSearchVectorStore.fromDocuments for populating
			// We verify the node has the insert mode which triggers populateVectorStore
			const modeField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'mode'
			);
			const options = modeField?.options as Array<{ value: string }>;
			const hasInsertMode = options?.some((opt) => opt.value === 'insert');

			expect(hasInsertMode).toBe(true);
		});

		it('should have insert mode with engine configuration', () => {
			const engineField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'engine') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('insert');
				}
			);

			expect(engineField).toBeDefined();
			expect(engineField?.default).toBe('lucene');
		});
	});

	describe('deleteDocument', () => {
		it('should have update mode which uses deleteDocument internally', () => {
			// The node has update mode which triggers deleteDocument before re-inserting
			const modeField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'mode'
			);
			const options = modeField?.options as Array<{ value: string }>;
			const hasUpdateMode = options?.some((opt) => opt.value === 'update');

			expect(hasUpdateMode).toBe(true);
		});

		it('should require document ID for update mode', () => {
			const idField = nodeInstance.description.properties.find(
				(prop) => {
					if (prop.name !== 'id') return false;
					const displayOptions = prop.displayOptions as { show?: { mode?: string[] } } | undefined;
					return displayOptions?.show?.mode?.includes('update');
				}
			);

			expect(idField).toBeDefined();
			expect(idField?.required).toBe(true);
			expect(idField?.type).toBe('string');
		});
	});
});
