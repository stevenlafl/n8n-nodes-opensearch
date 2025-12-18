import { OpenSearch } from './OpenSearch.node';

describe('OpenSearch Node', () => {
	describe('Node Description', () => {
		let nodeInstance: OpenSearch;

		beforeAll(() => {
			nodeInstance = new OpenSearch();
		});

		it('should have correct display name', () => {
			expect(nodeInstance.description.displayName).toBe('OpenSearch');
		});

		it('should have correct node name', () => {
			expect(nodeInstance.description.name).toBe('opensearch');
		});

		it('should require openSearchApi credentials', () => {
			expect(nodeInstance.description.credentials).toContainEqual(
				expect.objectContaining({
					name: 'openSearchApi',
					required: true,
				})
			);
		});

		it('should have document and index resources', () => {
			const resourceField = nodeInstance.description.properties.find(
				(prop) => prop.name === 'resource'
			);

			expect(resourceField).toBeDefined();
			expect(resourceField?.type).toBe('options');

			const options = resourceField?.options as Array<{ value: string }>;
			const values = options?.map((opt) => opt.value);

			expect(values).toContain('document');
			expect(values).toContain('index');
		});
	});

	describe('Document Operations', () => {
		let nodeInstance: OpenSearch;

		beforeAll(() => {
			nodeInstance = new OpenSearch();
		});

		it('should have all document operations', () => {
			const operationField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'operation' &&
					prop.displayOptions?.show?.resource?.includes('document')
			);

			expect(operationField).toBeDefined();

			const options = operationField?.options as Array<{ value: string }>;
			const operations = options?.map((opt) => opt.value);

			expect(operations).toContain('create');
			expect(operations).toContain('delete');
			expect(operations).toContain('get');
			expect(operations).toContain('getAll');
			expect(operations).toContain('search');
			expect(operations).toContain('update');
		});

		it('should have bulk operation options for create', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('create')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const bulkOption = options?.find((opt) => opt.name === 'bulkOperation');

			expect(bulkOption).toBeDefined();
		});

		it('should have bulk operation options for delete', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('delete')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const bulkOption = options?.find((opt) => opt.name === 'bulkOperation');

			expect(bulkOption).toBeDefined();
		});

		it('should have bulk operation options for update', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('update')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const bulkOption = options?.find((opt) => opt.name === 'bulkOperation');

			expect(bulkOption).toBeDefined();
		});

		it('should have scroll options for getAll', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('getAll')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const scrollOption = options?.find((opt) => opt.name === 'useScroll');
			const scrollTimeOption = options?.find((opt) => opt.name === 'scrollTime');

			expect(scrollOption).toBeDefined();
			expect(scrollTimeOption).toBeDefined();
		});

		it('should have scroll options for search', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('search')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const scrollOption = options?.find((opt) => opt.name === 'useScroll');
			const scrollTimeOption = options?.find((opt) => opt.name === 'scrollTime');

			expect(scrollOption).toBeDefined();
			expect(scrollTimeOption).toBeDefined();
		});

		it('should have simplify option for get operation', () => {
			const simpleField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'simple' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('get')
			);

			expect(simpleField).toBeDefined();
			expect(simpleField?.type).toBe('boolean');
			expect(simpleField?.default).toBe(true);
		});

		it('should have dataToSend options for create', () => {
			const dataToSendField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'dataToSend' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('create')
			);

			expect(dataToSendField).toBeDefined();

			const options = dataToSendField?.options as Array<{ value: string }>;
			const values = options?.map((opt) => opt.value);

			expect(values).toContain('defineBelow');
			expect(values).toContain('autoMapInputData');
		});
	});

	describe('Index Operations', () => {
		let nodeInstance: OpenSearch;

		beforeAll(() => {
			nodeInstance = new OpenSearch();
		});

		it('should have all index operations', () => {
			const operationField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'operation' &&
					prop.displayOptions?.show?.resource?.includes('index')
			);

			expect(operationField).toBeDefined();

			const options = operationField?.options as Array<{ value: string }>;
			const operations = options?.map((opt) => opt.value);

			expect(operations).toContain('create');
			expect(operations).toContain('delete');
			expect(operations).toContain('get');
			expect(operations).toContain('getAll');
		});

		it('should have additional fields for index create', () => {
			const additionalFieldsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'additionalFields' &&
					prop.displayOptions?.show?.resource?.includes('index') &&
					prop.displayOptions?.show?.operation?.includes('create')
			);

			expect(additionalFieldsField).toBeDefined();

			const options = additionalFieldsField?.options as Array<{ name: string }>;
			const fieldNames = options?.map((opt) => opt.name);

			expect(fieldNames).toContain('aliases');
			expect(fieldNames).toContain('mappings');
			expect(fieldNames).toContain('settings');
		});

		it('should have skipIfExists option for index create', () => {
			const additionalFieldsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'additionalFields' &&
					prop.displayOptions?.show?.resource?.includes('index') &&
					prop.displayOptions?.show?.operation?.includes('create')
			);

			expect(additionalFieldsField).toBeDefined();

			const options = additionalFieldsField?.options as Array<{ name: string }>;
			const skipIfExistsOption = options?.find((opt) => opt.name === 'skipIfExists');

			expect(skipIfExistsOption).toBeDefined();
		});

		it('should have skipIfNotExists option for index delete', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('index') &&
					prop.displayOptions?.show?.operation?.includes('delete')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const skipIfNotExistsOption = options?.find((opt) => opt.name === 'skipIfNotExists');

			expect(skipIfNotExistsOption).toBeDefined();
		});

		it('should have returnAll and limit for index getAll', () => {
			const returnAllField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'returnAll' &&
					prop.displayOptions?.show?.resource?.includes('index') &&
					prop.displayOptions?.show?.operation?.includes('getAll')
			);

			const limitField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'limit' &&
					prop.displayOptions?.show?.resource?.includes('index') &&
					prop.displayOptions?.show?.operation?.includes('getAll')
			);

			expect(returnAllField).toBeDefined();
			expect(limitField).toBeDefined();
		});
	});

	describe('Query Options', () => {
		let nodeInstance: OpenSearch;

		beforeAll(() => {
			nodeInstance = new OpenSearch();
		});

		it('should have query field for search operation', () => {
			const queryField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'query' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('search')
			);

			expect(queryField).toBeDefined();
			expect(queryField?.type).toBe('string');
			expect(queryField?.required).toBe(true);
		});

		it('should have query option in getAll options', () => {
			const optionsField = nodeInstance.description.properties.find(
				(prop) =>
					prop.name === 'options' &&
					prop.displayOptions?.show?.resource?.includes('document') &&
					prop.displayOptions?.show?.operation?.includes('getAll')
			);

			expect(optionsField).toBeDefined();

			const options = optionsField?.options as Array<{ name: string }>;
			const queryOption = options?.find((opt) => opt.name === 'query');

			expect(queryOption).toBeDefined();
		});
	});
});
