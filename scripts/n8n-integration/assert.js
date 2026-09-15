// Reads the execution JSON printed by dump-execution.js on stdin and checks the workflow in
// workflow.json did what it should. Exits non-zero with a reason on the first problem.
const chunks = [];
process.stdin.on('data', (chunk) => chunks.push(chunk));
process.stdin.on('end', () => {
	const text = Buffer.concat(chunks).toString('utf8');
	let run;
	try {
		run = JSON.parse(text);
	} catch (error) {
		fail(`could not parse execution dump: ${error.message}\n${text.slice(0, 2000)}`);
	}

	const result = run.resultData;
	if (!result) fail('no resultData in execution dump');
	if (result.error) fail(`workflow failed: ${result.error.message ?? JSON.stringify(result.error)}`);

	const runData = result.runData ?? {};
	const expectedNodes = [
		'Index: Create',
		'Index: Get',
		'Index: Get All',
		'Document: Create',
		'Document: Get',
		'Document: Update',
		'Document: Get Updated',
		'Document: Get All',
		'Document: Search',
		'Document: Delete',
		'Index: Delete',
		'Vector Store: Insert',
		'Vector Store: Load',
		'Vector Store: Update',
		'Vector Store: Load Owls',
		'Agent: OpenSearch Tool',
		'Agent: Vector Store Tool',
		'QA Chain',
	];
	for (const name of expectedNodes) {
		if (!runData[name]?.length) fail(`node "${name}" did not run`);
		const last = runData[name][runData[name].length - 1];
		if (last.error) fail(`node "${name}" errored: ${last.error.message}`);
	}

	const items = (name) => runData[name][runData[name].length - 1].data.main[0].map((item) => item.json);
	const first = (name) => items(name)[0] ?? {};

	const created = first('Document: Create');
	if (!created._id) fail(`Document: Create returned no _id: ${JSON.stringify(created)}`);

	const fetched = first('Document: Get');
	if ((fetched._source ?? fetched).title !== 'Test Document') {
		fail(`Document: Get returned ${JSON.stringify(fetched)}`);
	}

	const updated = first('Document: Get Updated');
	if ((updated._source ?? updated).title !== 'Updated Document') {
		fail(`Document: Get Updated returned ${JSON.stringify(updated)}`);
	}

	if (!items('Index: Get All').some((index) => index.indexId === 'it-documents')) {
		fail(`Index: Get All did not list it-documents: ${JSON.stringify(items('Index: Get All')).slice(0, 300)}`);
	}
	if (items('Document: Get All').length !== 1) {
		fail(`Document: Get All returned ${items('Document: Get All').length} items, expected 1`);
	}
	const found = first('Document: Search');
	if ((found._source ?? found).title !== 'Updated Document') {
		fail(`Document: Search returned ${JSON.stringify(found)}`);
	}

	if (items('Vector Store: Insert').length !== 3) {
		fail(`Vector Store: Insert returned ${items('Vector Store: Insert').length} items, expected 3`);
	}

	const loaded = first('Vector Store: Load');
	const pageContent = loaded.document?.pageContent ?? '';
	if (!pageContent.includes('cats')) {
		fail(`Vector Store: Load for "cats" returned ${JSON.stringify(loaded)}`);
	}

	const owls = first('Vector Store: Load Owls');
	if (!(owls.document?.pageContent ?? '').includes('owls')) {
		fail(`Vector Store: Load Owls returned ${JSON.stringify(owls)}`);
	}

	// The stub chat model calls the first tool it is offered, so the tool sub-nodes must have run
	// and their result must have come back through the agent
	for (const tool of ['OpenSearch Tool', 'Vector Store: As Tool']) {
		if (!runData[tool]?.length) fail(`tool "${tool}" was never called`);
	}
	for (const agent of ['Agent: OpenSearch Tool', 'Agent: Vector Store Tool']) {
		const output = first(agent).output ?? '';
		if (!output.includes('cats')) fail(`${agent} output does not mention cats: ${JSON.stringify(first(agent)).slice(0, 300)}`);
	}
	if (!runData['Vector Store: Retrieve']?.length) fail('Vector Store: Retrieve was never used by the retriever');
	const answer = first('QA Chain').response ?? first('QA Chain').output ?? '';
	if (!String(answer).includes('cats')) fail(`QA Chain response does not mention cats: ${JSON.stringify(first('QA Chain')).slice(0, 300)}`);
});

function fail(message) {
	console.error(`assert: ${message}`);
	process.exit(1);
}
