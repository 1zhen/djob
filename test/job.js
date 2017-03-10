/**
 * Created by nicholas on 17-3-7.
 */
const Job = require('../lib/job');
const Etcd = require('etcd-cli');
const {expect} = require('chai');
const Client = require('../lib/client');

describe('Test job', () => {
    let etcd = new Client('127.0.0.1:2379');
    it('Test start job', function (done) {
        this.timeout(15000);
        let times = 0;
        let job = new Job(etcd, {
            namespace: 'dJobTest',
            name: 'testStartJob',
            cron: '*/1 * * * * *',
            shards: 1
        }, (shardInfo) => {
            if (++times === 5) {
                job.stop().then(() => {
                    done();
                });
            }
        });
        job.start().then();
    });
    it('Test stop job', function (done) {
        this.timeout(15000);
        let started = false;
        let stopped = false;
        let runAfterStopped = 0;
        let job = new Job(etcd, {
            namespace: 'dJobTest',
            name: 'testStopJob',
            cron: '*/1 * * * * *',
            shards: 1
        }, (shardInfo) => {
            if (!started) {
                started = true;
                job.stop().then(() => {
                    stopped = true;
                    setTimeout(() => {
                        expect(runAfterStopped).to.be.equal(0);
                        done();
                    }, 3000);
                }).catch(done);
            } else if (stopped) {
                runAfterStopped ++;
            }
        });
        job.start();
    });
});