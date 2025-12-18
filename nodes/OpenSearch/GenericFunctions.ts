import type {
	IExecuteFunctions,
	IDataObject,
	JsonObject,
	IHttpRequestOptions,
	IHttpRequestMethods,
} from 'n8n-workflow';
import { NodeApiError } from 'n8n-workflow';

import type { OpenSearchApiCredentials } from './types';

/**
 * Executes a bulk API request to OpenSearch
 * @param body - Object containing bulk operations, each value will be joined with newlines
 * @returns Array of operation results for each item in the bulk request
 */
export async function openSearchBulkApiRequest(this: IExecuteFunctions, body: IDataObject) {
	const { baseUrl, ignoreSSLIssues } = (await this.getCredentials(
		'openSearchApi',
	)) as OpenSearchApiCredentials;

	const normalizedUrl = baseUrl.replace(/\/$/, '');

	const bulkBody = `${Object.values(body).flat().join('\n')}\n`;

	const options: IHttpRequestOptions = {
		method: 'POST',
		headers: { 'Content-Type': 'application/x-ndjson' },
		body: bulkBody,
		url: `${normalizedUrl}/_bulk`,
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
		}
		throw new NodeApiError(this.getNode(), { error: response.body.error } as JsonObject);
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

/**
 * Executes a single API request to OpenSearch
 * @param method - HTTP method (GET, POST, PUT, DELETE, HEAD)
 * @param endpoint - API endpoint path (e.g., '/index/_doc/1')
 * @param body - Request body data
 * @param qs - Query string parameters
 * @returns Response data from OpenSearch
 */
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

	const normalizedUrl = baseUrl.replace(/\/$/, '');

	const options: IHttpRequestOptions = {
		method,
		url: `${normalizedUrl}${endpoint}`,
		json: method !== 'HEAD', // HEAD requests don't return a body, so disable JSON parsing
		skipSslCertificateValidation: ignoreSSLIssues,
		...(Object.keys(body).length && { body }),
		...(Object.keys(qs).length && { qs }),
	};

	try {
		return await this.helpers.httpRequestWithAuthentication.call(this, 'openSearchApi', options);
	} catch (error) {
		const err = error as JsonObject & { statusCode?: number; message?: string };

		// Provide helpful message for 400 errors which often indicate malformed queries
		if (err.statusCode === 400) {
			throw new NodeApiError(this.getNode(), err, {
				message: 'Bad request - please check your parameters',
				description: 'OpenSearch rejected the query. If using as an AI tool, the AI may have sent an invalid query format. Expected format: {"query": {"match_all": {}}} or similar OpenSearch Query DSL.',
			});
		}

		throw new NodeApiError(this.getNode(), err);
	}
}

/**
 * Fetches all items from an index using Point-in-Time (PIT) pagination
 * Automatically handles pagination to retrieve all matching documents
 * @param indexId - The index to search
 * @param body - Search query body
 * @param qs - Query string parameters
 * @returns Array of all matching documents
 */
export async function openSearchApiRequestAllItems(
	this: IExecuteFunctions,
	indexId: string,
	body: IDataObject = {},
	qs: IDataObject = {},
): Promise<IDataObject[]> {
	//https://www.elastic.co/guide/en/elasticsearch/reference/7.16/paginate-search-results.html#search-after
	try {
		//create a point in time (PIT) to preserve the current index state over your searches
		let pit = (
			await openSearchApiRequest.call(this, 'POST', `/${indexId}/_pit`, {}, { keep_alive: '1m' })
		)?.id as string;

		let returnData: IDataObject[] = [];
		// biome-ignore lint/suspicious/noExplicitAny: responseData structure varies during pagination
		let responseData: any;
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

		// Always use POST for PIT searches as they require a body
		responseData = await openSearchApiRequest.call(this, 'POST', '/_search', requestBody, qs);
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

			responseData = await openSearchApiRequest.call(this, 'POST', '/_search', requestBody, qs);

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

/**
 * Fetches all items from an index using Scroll API pagination
 * Alternative to PIT pagination, useful for older OpenSearch versions
 * @param indexId - The index to search
 * @param body - Search query body
 * @param qs - Query string parameters
 * @param scrollTime - Time to keep scroll context alive in minutes (default: 1)
 * @returns Array of all matching documents
 */
export async function openSearchApiRequestWithScroll(
	this: IExecuteFunctions,
	indexId: string,
	body: IDataObject = {},
	qs: IDataObject = {},
	scrollTime: number = 1,
): Promise<IDataObject[]> {
	try {
		let returnData: IDataObject[] = [];
		// biome-ignore lint/suspicious/noExplicitAny: responseData structure varies during pagination
		let responseData: any;
		let scrollId: string | undefined;

		// Initial search request with scroll
		const scrollTimeString = `${scrollTime}m`;
		const initialQs = { ...qs, scroll: scrollTimeString };

		// Determine if we need POST (complex query) or can use GET (simple or no query)
		const hasComplexQuery = Object.keys(body).length > 0;
		const method = hasComplexQuery ? 'POST' : 'GET';

		responseData = await openSearchApiRequest.call(this, method, `/${indexId}/_search`, hasComplexQuery ? body : {}, initialQs);
		
		if (responseData?.hits?.hits) {
			returnData = returnData.concat(responseData.hits.hits as IDataObject[]);
			scrollId = responseData._scroll_id;
		} else {
			return [];
		}

		// Continue scrolling until no more results
		while (scrollId && responseData?.hits?.hits?.length > 0) {
			const scrollBody = {
				scroll: scrollTimeString,
				scroll_id: scrollId,
			};

			responseData = await openSearchApiRequest.call(this, 'GET', '/_search/scroll', scrollBody);

			if (responseData?.hits?.hits?.length > 0) {
				returnData = returnData.concat(responseData.hits.hits as IDataObject[]);
				scrollId = responseData._scroll_id;
			} else {
				break;
			}
		}

		// Clear scroll context
		if (scrollId) {
			try {
				await openSearchApiRequest.call(this, 'DELETE', '/_search/scroll', { scroll_id: scrollId });
			} catch (error) {
				// Ignore cleanup errors
			}
		}

		return returnData;
	} catch (error) {
		throw new NodeApiError(this.getNode(), error as JsonObject);
	}
}
