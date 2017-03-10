/**
 * Created by nicholas on 17-3-10.
 */
const Job = require('../lib/job');
const Client = require('../lib/client');

let client = new Client('127.0.0.1:2379');
let job = new Job(client, {
    namespace: 'dJobTest',
    name: 'testDeadJob',
    cron: '*/1 * * * * *',
    shards: 1
}, (shardInfo) => {
    return new Promise((resolve, reject) => {
        process.send('');
        setTimeout(() => {
            resolve();
        }, 3000);
    })
});
job.start();