/**
 * Created by nicholas on 17-3-7.
 */
const Shard = require('./shard');
const parser = require('cron-parser');
const async = require('async');
const _ = require('lodash');
const path = require('path');
const bunyan = require('bunyan');
const Promise = require('bluebird');

function Job(client, options, executor) {
    _.defaults(options, {
        namespace: 'djob',
        shards: 1
    });
    if (!options.logger) {
        options.logger = bunyan.createLogger({
            name: 'job-' + options.namespace + '/' + options.name
        });
    }
    this.$logger = options.logger;
    let jobInfo = {
        namespace: options.namespace,
        name: options.name,
        cron: options.cron,
        shards: options.shards
    };
    this.$jobInfo = jobInfo;
    this.$etcd = client.etcd;
    this.$client = client;
    this.name = jobInfo.name;
    this.$shards = [];
    this.$executor = executor;
}

Job.prototype.schedule = function () {
    if (this.isSchedulable()) {
        //Calculate next timeout
        let nextTime;
        let currentTime = new Date();
        do {
            nextTime = this.$interval.next().toDate();
        } while (nextTime < currentTime);
        this.$shards.forEach((shard) => {
            shard.schedule(this.$executor, nextTime - currentTime);
        });
    }
};

Job.prototype.isSchedulable = function () {
    return this.isAllShardsFinished() && this.hasMasterShard();
};

Job.prototype.isAllShardsFinished = function () {
    let result = true;
    this.$shards.forEach((shard) => {
        result &= shard.isFinished();
    });
    return result;
};

Job.prototype.hasMasterShard = function () {
    for (let i = 0; i < this.$shards.length; i ++) {
        if (this.$shards[i].isMaster()) {
            return true;
        }
    }
    return false;
};

/*
 * @returns {Promise}
 */
Job.prototype.start = function () {
    let jobInfoKey = path.join(this.$jobInfo.namespace, 'jobs', this.$jobInfo.name, 'info');
    let jobInfoIndex = 0;
    let heartbeatKey = path.join(this.$jobInfo.namespace, 'jobs', this.$jobInfo.name, 'clients', this.$client.id);
    let promises = [
        this.$etcd.set(heartbeatKey, JSON.stringify({
            startAt: new Date().getTime()
        }), {
            prevExist: false,
            ttl: 30
        }).then(() => {
            this.$heartbeatTimer = setInterval(() => {
                this.$logger.debug('Job heartbeat.');
                this.$etcd.ttl(heartbeatKey, 30);
            }, 10000);
            this.$logger.info('Job heartbeat started.');
        }),
        this.$etcd.get(jobInfoKey).then((data) => {
            jobInfoIndex = data.node.modifiedIndex;
            try {
                this.$jobInfo = JSON.parse(data.node.value);
            } catch (e) {
                this.emit('error', e);
            }
        }).catch((err) => {
            return this.$etcd.set(jobInfoKey, JSON.stringify(this.$jobInfo), {
                prevExist: false
            }).then((data) => {
                jobInfoIndex = data.node.modifiedIndex;
            }).catch((err) => {
                return this.$etcd.get(jobInfoKey).then((data) => {
                    jobInfoIndex = data.node.modifiedIndex;
                    try {
                        this.$jobInfo = JSON.parse(data.node.value);
                    } catch (e) {
                        this.emit('error', e);
                    }
                });
            });
        }).then(this.$etcd.watcher(jobInfoKey, jobInfoIndex + 1).then((watcher) => {
            this.$jobInfoWatcher = watcher;
            watcher.on('change', (data) => {
                this.$logger.info('Job info changed, restart job to apply new settings.');
                try {
                    this.$jobInfo = JSON.parse(data.node.value);
                    this.stop((err) => {
                        if (err) {
                            return this.emit('error', err);
                        }
                        this.start((err) => {
                            if (err) {
                                return this.emit('error', err);
                            }
                        });
                    })
                } catch (e) {
                    this.emit('error');
                }
            });
            return watcher.start();
        })).then(() => {
            this.$interval = parser.parseExpression(this.$jobInfo.cron);
            this.$shards = [];
            let shardStarts = [];
            for (let i = 0; i < this.$jobInfo.shards; i ++) {
                let shard = new Shard(this.$client, {
                    namespace: this.$jobInfo.namespace,
                    jobName: this.name,
                    shardInfo: {
                        index: i
                    }
                });
                shard.on('master', (shardInfo) => {
                    this.schedule();
                });
                shard.on('status', (status) => {
                    switch (status) {
                        case 'completed':
                        case 'ready':
                            if (this.isAllShardsFinished()) {
                                //Schedule next run
                                this.schedule();
                            }
                            break;
                    }
                });
                this.$shards.push(shard);
                shardStarts.push(shard.start());
            }
            return Promise.all(shardStarts);
        })
    ];

    return Promise.all(promises).then(() => {
        this.$logger.info('Job started.');
    }).catch((err) => {
        this.$logger.error('Job start with error.', err);
        throw err;
    });
};

/**
 *
 * @returns {Promise}
 */
Job.prototype.stop = function () {
    let promises = [
        this.$jobInfoWatcher.stop().then(() => {
            delete this.$jobInfoWatcher;
        })
    ];
    this.$shards.forEach((shard) => {
        promises.push(shard.stop());
    });
    return Promise.all(promises).then(() => {
        this.$logger.info('Job stopped.');
    }).catch((err) => {
        this.$logger.error('Job stopped with error.', err);
        throw err;
    }).finally(() => {
        if (this.$heartbeatTimer) {
            clearInterval(this.$heartbeatTimer);
            delete this.$heartbeatTimer;
            this.$logger.info('Job heartbeat stopped.');
        }
    });
};

module.exports = Job;