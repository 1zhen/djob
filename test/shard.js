/**
 * Created by nicholas on 17-3-8.
 */
const Shard = require('../lib/shard');
const Etcd = require('etcd-cli');
const Client = require('../lib/client');

describe('Test shard', () => {
    let etcd = new Client('127.0.0.1:2379');
    it('Test election', function (done) {
        let shard = new Shard(etcd, {
            namespace: 'djob',
            jobName: 'testElectionJob',
            shardInfo: {
                index: 0
            }
        });
        shard.on('master', () => {
            done();
        });
        shard.on('slave', () => {
            done();
        });
    });

    it('Test job status', function (done) {
        let shard = new Shard(etcd, {
            namespace: 'djob',
            jobName: 'testStatusJob',
            shardInfo: {
                index: 0
            }
        });
        let count = 0;
        shard.on('status', (status) => {
            if (++ count === 3) {
                done();
            }
        });
        shard.finish();
        shard.fail();
    })
});