import type {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class OpenSearchApi implements ICredentialType {
	name = 'openSearchApi';

	displayName = 'OpenSearch API';

	documentationUrl = 'https://docs.n8n.io/credentials/openSearchApi/';

	icon = { light: 'file:opensearch.svg', dark: 'file:opensearch.dark.svg' } as const;

	properties: INodeProperties[] = [
		{
			displayName: 'Username',
			name: 'username',
			type: 'string',
			default: '',
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
		},
		{
			displayName: 'Base URL',
			name: 'baseUrl',
			type: 'string',
			default: '',
			placeholder: 'e.g. https://mydeployment.es.us-central1.gcp.cloud.es.io:9243',
			description: 'The base URL of your OpenSearch cluster',
		},
		{
			displayName: 'Ignore SSL Issues',
			name: 'ignoreSSLIssues',
			type: 'boolean',
			default: false,
			description: 'Whether to ignore SSL certificate validation issues',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			auth: {
				username: '={{$credentials.username}}',
				password: '={{$credentials.password}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: '={{$credentials.baseUrl}}',
			url: '/_count?human=false',
			skipSslCertificateValidation: '={{$credentials.ignoreSSLIssues}}',
		},
	};
}
