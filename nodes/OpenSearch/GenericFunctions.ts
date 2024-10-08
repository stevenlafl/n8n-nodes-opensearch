import type {
	IExecuteFunctions,
	IDataObject,
	JsonObject,
	IHttpRequestOptions,
	IHttpRequestMethods,
} from 'n8n-workflow';
import { NodeApiError } from 'n8n-workflow';

import type { OpenSearchApiCredentials } from './types';

export async function openSearchBulkApiRequest(this: IExecuteFunctions, body: IDataObject) {
	const { baseUrl, ignoreSSLIssues } = (await this.getCredentials(
		'openSearchApi',
	)) as OpenSearchApiCredentials;

	// biome-ignore lint/style/useTemplate: <explanation>
	const bulkBody = Object.values(body).flat().join('\n') + '\n';

	const options: IHttpRequestOptions = {
		method: 'POST',
		headers: { 'Content-Type': 'application/x-ndjson' },
		body: bulkBody,
		url: `${baseUrl}/_bulk`,
		skipSslCertificateValidation: ignoreSSLIssues,
		returnFullResponse: true,
		ignoreHttpStatusErrors: true,
	};

	const response = await this.helpers.httpRequestWithAuthentication.call(
		this,
		'openSearchApi',
		options,
	);

	if (response.statusCode > 299) {
		if (this.continueOnFail()) {
			return Object.values(body).map((_) => ({ error: response.body.error }));
		// biome-ignore lint/style/noUselessElse: <explanation>
		} else {
			throw new NodeApiError(this.getNode(), { error: response.body.error } as JsonObject);
		}
	}

	return response.body.items.map((item: IDataObject) => {
		return {
			...(item.index as IDataObject),
			...(item.update as IDataObject),
			...(item.create as IDataObject),
			...(item.delete as IDataObject),
			...(item.error as IDataObject),
		};
	});
}

export async function openSearchApiRequest(
	this: IExecuteFunctions,
	method: IHttpRequestMethods,
	endpoint: string,
	body: IDataObject = {},
	qs: IDataObject = {},
) {
	const { baseUrl, ignoreSSLIssues } = (await this.getCredentials(
		'openSearchApi',
	)) as OpenSearchApiCredentials;

	const options: IHttpRequestOptions = {
		method,
		body,
		qs,
		url: `${baseUrl}${endpoint}`,
		json: true,
		skipSslCertificateValidation: ignoreSSLIssues,
	};

	if (!Object.keys(body).length) {
		// biome-ignore lint/performance/noDelete: <explanation>
		delete options.body;
	}

	if (!Object.keys(qs).length) {
		// biome-ignore lint/performance/noDelete: <explanation>
		delete options.qs;
	}

	try {
		return await this.helpers.httpRequestWithAuthentication.call(this, 'openSearchApi', options);
	} catch (error) {
		throw new NodeApiError(this.getNode(), error as JsonObject);
	}
}

export async function openSearchApiRequestAllItems(
	this: IExecuteFunctions,
	indexId: string,
	body: IDataObject = {},
	qs: IDataObject = {},
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
): Promise<any> {
	//https://www.elastic.co/guide/en/elasticsearch/reference/7.16/paginate-search-results.html#search-after
	try {
		//create a point in time (PIT) to preserve the current index state over your searches
		let pit = (
			await openSearchApiRequest.call(this, 'POST', `/${indexId}/_pit`, {}, { keep_alive: '1m' })
		)?.id as string;

		let returnData: IDataObject[] = [];
		// biome-ignore lint/suspicious/noImplicitAnyLet: <explanation>
		let responseData;
		let searchAfter: string[] = [];

		const requestBody: IDataObject = {
			...body,
			size: 10000,
			pit: {
				id: pit,
				keep_alive: '1m',
			},
			track_total_hits: false, //Disable the tracking of total hits to speed up pagination
		};

		responseData = await openSearchApiRequest.call(this, 'GET', '/_search', requestBody, qs);
		if (responseData?.hits?.hits) {
			returnData = returnData.concat(responseData.hits.hits as IDataObject[]);
			const lastHitIndex = responseData.hits.hits.length - 1;
			//Sort values for the last returned hit with the tiebreaker value
			searchAfter = responseData.hits.hits[lastHitIndex].sort;
			//Update id for the point in time
			pit = responseData.pit_id;
		} else {
			return [];
		}

		while (true) {
			requestBody.search_after = searchAfter;
			requestBody.pit = { id: pit, keep_alive: '1m' };

			responseData = await openSearchApiRequest.call(this, 'GET', '/_search', requestBody, qs);

			if (responseData?.hits?.hits?.length) {
				returnData = returnData.concat(responseData.hits.hits as IDataObject[]);
				const lastHitIndex = responseData.hits.hits.length - 1;
				searchAfter = responseData.hits.hits[lastHitIndex].sort;
				pit = responseData.pit_id;
			} else {
				break;
			}
		}

		await openSearchApiRequest.call(this, 'DELETE', '/_pit', { id: pit });

		return returnData;
	} catch (error) {
		throw new NodeApiError(this.getNode(), error as JsonObject);
	}
}
