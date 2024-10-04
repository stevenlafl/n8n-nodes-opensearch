/* eslint-disable n8n-nodes-base/node-filename-against-convention */
import type {
	IExecuteFunctions,
	IDataObject,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	JsonObject,
} from 'n8n-workflow';
import { jsonParse, NodeApiError, NodeConnectionType } from 'n8n-workflow';

import omit from 'lodash/omit';
import {
	openSearchApiRequest,
	openSearchApiRequestAllItems,
	openSearchBulkApiRequest,
} from './GenericFunctions';

import { documentFields, documentOperations, indexFields, indexOperations } from './descriptions';

import type { DocumentGetAllOptions, FieldsUiValues } from './types';

export class OpenSearch implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenSearch',
		name: 'opensearch',
		icon: 'file:opensearch.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Consume the OpenSearch API',
		defaults: {
			name: 'OpenSearch',
		},
		// eslint-disable-next-line n8n-nodes-base/node-class-description-inputs-wrong-regular-node
		inputs: [NodeConnectionType.Main],
		// eslint-disable-next-line n8n-nodes-base/node-class-description-outputs-wrong
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'openSearchApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Document',
						value: 'document',
					},
					{
						name: 'Index',
						value: 'index',
					},
				],
				default: 'document',
			},
			...documentOperations,
			...documentFields,
			...indexOperations,
			...indexFields,
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		const resource = this.getNodeParameter('resource', 0) as 'document' | 'index';
		const operation = this.getNodeParameter('operation', 0);

		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		let responseData: any;

		let bulkBody: IDataObject = {};

		for (let i = 0; i < items.length; i++) {
			const bulkOperation = this.getNodeParameter('options.bulkOperation', i, false);
			if (resource === 'document') {
				// **********************************************************************
				//                                document
				// **********************************************************************
				if (operation === 'delete') {
					// ----------------------------------------
					//             document: delete
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/docs-delete.html

					const indexId = this.getNodeParameter('indexId', i);
					const documentId = this.getNodeParameter('documentId', i);

					if (bulkOperation) {
						bulkBody[i] = JSON.stringify({
							delete: {
								_index: indexId,
								_id: documentId,
							},
						});
					} else {
						const endpoint = `/${indexId}/_doc/${documentId}`;
						responseData = await openSearchApiRequest.call(this, 'DELETE', endpoint);
					}
				} else if (operation === 'get') {
					// ----------------------------------------
					//              document: get
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/docs-get.html

					const indexId = this.getNodeParameter('indexId', i);
					const documentId = this.getNodeParameter('documentId', i);

					const qs = {} as IDataObject;
					const options = this.getNodeParameter('options', i);

					if (Object.keys(options).length) {
						Object.assign(qs, options);
						qs._source = true;
					}

					const endpoint = `/${indexId}/_doc/${documentId}`;
					responseData = await openSearchApiRequest.call(this, 'GET', endpoint, {}, qs);

					const simple = this.getNodeParameter('simple', i) as IDataObject;

					if (simple) {
						responseData = {
							_id: responseData._id,
							...responseData._source,
						};
					}
				} else if (operation === 'getAll') {
					// ----------------------------------------
					//            document: getAll
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/search-search.html

					const indexId = this.getNodeParameter('indexId', i);

					const body = {} as IDataObject;
					const qs = {} as IDataObject;
					const options = this.getNodeParameter('options', i) as DocumentGetAllOptions;
					// const paginate = this.getNodeParameter('paginate', i) as boolean;

					if (Object.keys(options).length) {
						const { query, ...rest } = options;
						if (query) {
							Object.assign(
								body,
								jsonParse(query, { errorMessage: "Invalid JSON in 'Query' option" }),
							);
						}
						Object.assign(qs, rest);
						qs._source = true;
					}

					const returnAll = this.getNodeParameter('returnAll', 0);

					if (returnAll) {
						//Defines the number of hits to return. Defaults to 10. By default, you cannot page through more than 10,000 hits
						qs.size = 10000;
						if (qs.sort) {
							responseData = await openSearchApiRequestAllItems.call(
								this,
								indexId as string,
								body,
								qs,
							);
						} else {
							responseData = await openSearchApiRequest.call(
								this,
								'GET',
								`/${indexId}/_search`,
								body,
								qs,
							);
							responseData = responseData.hits.hits;
						}
					} else {
						qs.size = this.getNodeParameter('limit', 0);

						responseData = await openSearchApiRequest.call(
							this,
							'GET',
							`/${indexId}/_search`,
							body,
							qs,
						);
						responseData = responseData.hits.hits;
					}

					const simple = this.getNodeParameter('simple', 0) as IDataObject;

					if (simple) {
						responseData = responseData.map((item: IDataObject) => {
							return {
								_id: item._id,
								...(item._source as IDataObject),
							};
						});
					}
				} else if (operation === 'create') {
					// ----------------------------------------
					//             document: create
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/docs-index_.html

					const body: IDataObject = {};

					const dataToSend = this.getNodeParameter('dataToSend', 0) as
						| 'defineBelow'
						| 'autoMapInputData';

					if (dataToSend === 'defineBelow') {
						const fields = this.getNodeParameter('fieldsUi.fieldValues', i, []) as FieldsUiValues;
						// biome-ignore lint/suspicious/noAssignInExpressions: <explanation>
						// biome-ignore lint/complexity/noForEach: <explanation>
						fields.forEach(({ fieldId, fieldValue }) => (body[fieldId] = fieldValue));
					} else {
						const inputData = items[i].json;
						const rawInputsToIgnore = this.getNodeParameter('inputsToIgnore', i) as string;
						const inputsToIgnore = rawInputsToIgnore.split(',').map((c) => c.trim());

						for (const key of Object.keys(inputData)) {
							if (inputsToIgnore.includes(key)) continue;
							body[key] = inputData[key];
						}
					}

					const qs = {} as IDataObject;
					const additionalFields = this.getNodeParameter('additionalFields', i);

					if (Object.keys(additionalFields).length) {
						Object.assign(qs, omit(additionalFields, ['documentId']));
					}

					const indexId = this.getNodeParameter('indexId', i);
					const { documentId } = additionalFields;

					if (bulkOperation) {
						bulkBody[i] = JSON.stringify({
							index: {
								_index: indexId,
								_id: documentId,
							},
						});
						bulkBody[i] += `\n${JSON.stringify(body)}`;
					} else {
						if (documentId) {
							const endpoint = `/${indexId}/_doc/${documentId}`;
							responseData = await openSearchApiRequest.call(this, 'PUT', endpoint, body);
						} else {
							const endpoint = `/${indexId}/_doc`;
							responseData = await openSearchApiRequest.call(this, 'POST', endpoint, body);
						}
					}
				} else if (operation === 'update') {
					// ----------------------------------------
					//             document: update
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/docs-update.html

					const body = { doc: {} } as { doc: { [key: string]: string } };

					const dataToSend = this.getNodeParameter('dataToSend', 0) as
						| 'defineBelow'
						| 'autoMapInputData';

					if (dataToSend === 'defineBelow') {
						const fields = this.getNodeParameter('fieldsUi.fieldValues', i, []) as FieldsUiValues;
						// biome-ignore lint/suspicious/noAssignInExpressions: <explanation>
						// biome-ignore lint/complexity/noForEach: <explanation>
						fields.forEach(({ fieldId, fieldValue }) => (body.doc[fieldId] = fieldValue));
					} else {
						const inputData = items[i].json;
						const rawInputsToIgnore = this.getNodeParameter('inputsToIgnore', i) as string;
						const inputsToIgnore = rawInputsToIgnore.split(',').map((c) => c.trim());

						for (const key of Object.keys(inputData)) {
							if (inputsToIgnore.includes(key)) continue;
							body.doc[key] = inputData[key] as string;
						}
					}

					const indexId = this.getNodeParameter('indexId', i);
					const documentId = this.getNodeParameter('documentId', i);

					const endpoint = `/${indexId}/_update/${documentId}`;
					if (bulkOperation) {
						bulkBody[i] = JSON.stringify({
							update: {
								_index: indexId,
								_id: documentId,
							},
						});
						bulkBody[i] += `\n${JSON.stringify(body)}`;
					} else {
						responseData = await openSearchApiRequest.call(this, 'POST', endpoint, body);
					}
				}
			} else if (resource === 'index') {
				// **********************************************************************
				//                                 index
				// **********************************************************************

				// https://www.elastic.co/guide/en/openSearch/reference/current/indices.html

				if (operation === 'create') {
					// ----------------------------------------
					//              index: create
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/indices-create-index.html

					const indexId = this.getNodeParameter('indexId', i);

					const body = {} as IDataObject;
					const qs = {} as IDataObject;
					const additionalFields = this.getNodeParameter('additionalFields', i);

					if (Object.keys(additionalFields).length) {
						const { aliases, mappings, settings, ...rest } = additionalFields;
						Object.assign(body, aliases, mappings, settings);
						Object.assign(qs, rest);
					}

					responseData = await openSearchApiRequest.call(this, 'PUT', `/${indexId}`);
					responseData = { id: indexId, ...responseData };
					// biome-ignore lint/performance/noDelete: <explanation>
					delete responseData.index;
				} else if (operation === 'delete') {
					// ----------------------------------------
					//              index: delete
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/indices-delete-index.html

					const indexId = this.getNodeParameter('indexId', i);

					responseData = await openSearchApiRequest.call(this, 'DELETE', `/${indexId}`);
					responseData = { success: true };
				} else if (operation === 'get') {
					// ----------------------------------------
					//              index: get
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/indices-get-index.html

					const indexId = this.getNodeParameter('indexId', i) as string;

					const qs = {} as IDataObject;
					const additionalFields = this.getNodeParameter('additionalFields', i);

					if (Object.keys(additionalFields).length) {
						Object.assign(qs, additionalFields);
					}

					responseData = await openSearchApiRequest.call(this, 'GET', `/${indexId}`, {}, qs);
					responseData = { id: indexId, ...responseData[indexId] };
				} else if (operation === 'getAll') {
					// ----------------------------------------
					//              index: getAll
					// ----------------------------------------

					// https://www.elastic.co/guide/en/openSearch/reference/current/indices-aliases.html

					responseData = await openSearchApiRequest.call(this, 'GET', '/_aliases');
					responseData = Object.keys(responseData as IDataObject).map((index) => ({
						indexId: index,
					}));

					const returnAll = this.getNodeParameter('returnAll', i);

					if (!returnAll) {
						const limit = this.getNodeParameter('limit', i);
						responseData = responseData.slice(0, limit);
					}
				}
			}

			if (!bulkOperation) {
				const executionData = this.helpers.constructExecutionMetaData(
					this.helpers.returnJsonArray(responseData as IDataObject[]),
					{ itemData: { item: i } },
				);
				returnData.push(...executionData);
			}
			if (Object.keys(bulkBody).length >= 50) {
				responseData = (await openSearchBulkApiRequest.call(this, bulkBody)) as IDataObject[];
				for (let j = 0; j < responseData.length; j++) {
					const itemData = responseData[j];
					if (itemData.error) {
						const errorData = itemData.error as IDataObject;
						const message = errorData.type as string;
						const description = errorData.reason as string;
						const itemIndex = Number.parseInt(Object.keys(bulkBody)[j]);
						if (this.continueOnFail()) {
							returnData.push(
								...this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray({ error: message, message: itemData.error }),
									{ itemData: { item: itemIndex } },
								),
							);
							continue;
						// biome-ignore lint/style/noUselessElse: <explanation>
						} else {
							throw new NodeApiError(this.getNode(), {
								message,
								description,
								itemIndex,
							} as JsonObject);
						}
					}
					const executionData = this.helpers.constructExecutionMetaData(
						this.helpers.returnJsonArray(itemData),
						{ itemData: { item: Number.parseInt(Object.keys(bulkBody)[j]) } },
					);
					returnData.push(...executionData);
				}
				bulkBody = {};
			}
		}
		if (Object.keys(bulkBody).length) {
			responseData = (await openSearchBulkApiRequest.call(this, bulkBody)) as IDataObject[];
			for (let j = 0; j < responseData.length; j++) {
				const itemData = responseData[j];
				if (itemData.error) {
					const errorData = itemData.error as IDataObject;
					const message = errorData.type as string;
					const description = errorData.reason as string;
					const itemIndex = Number.parseInt(Object.keys(bulkBody)[j]);
					if (this.continueOnFail()) {
						returnData.push(
							...this.helpers.constructExecutionMetaData(
								this.helpers.returnJsonArray({ error: message, message: itemData.error }),
								{ itemData: { item: itemIndex } },
							),
						);
						continue;
					// biome-ignore lint/style/noUselessElse: <explanation>
					} else {
						throw new NodeApiError(this.getNode(), {
							message,
							description,
							itemIndex,
						} as JsonObject);
					}
				}
				const executionData = this.helpers.constructExecutionMetaData(
					this.helpers.returnJsonArray(itemData),
					{ itemData: { item: Number.parseInt(Object.keys(bulkBody)[j]) } },
				);
				returnData.push(...executionData);
			}
		}
		return [returnData];
	}
}
