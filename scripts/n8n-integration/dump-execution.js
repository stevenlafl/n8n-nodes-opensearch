// Runs inside the n8n container after `n8n execute` and prints the latest execution as JSON,
// independent of n8n's log level. Needs NODE_PATH pointing at n8n's node_modules.
const sqlite3 = require('sqlite3');
const flatted = require('flatted');

const db = new sqlite3.Database(process.env.N8N_DATABASE ?? '/home/node/.n8n/database.sqlite');
db.get(
	'SELECT e.id, e.status, d.data FROM execution_entity e JOIN execution_data d ON d.executionId = e.id ORDER BY e.id DESC LIMIT 1',
	(error, row) => {
		if (error) throw error;
		if (!row) throw new Error('no execution found');
		const run = flatted.parse(row.data);
		console.log(JSON.stringify({ id: row.id, status: row.status, resultData: run.resultData }));
	},
);
