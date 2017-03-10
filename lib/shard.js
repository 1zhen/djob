/**
 * Created by nicholas on 17-3-7.
 */
const EventEmitter = require('events').EventEmitter;
const util = require('util');
const path = require('path');
const uuid = require('uuid');
const _ = require('lodash');
const bunyan = require('bunyan');
const async = require('async');
const Promise = require('bluebird');
const arguejs = require('arguejs');

/**
 *
 * @param {Client} client
 * @param {object} options
 * @constructor
 */
function Shard(client, options) {
    EventEmitter.call(this);
    _.defaults(options, {
        namespace: 'djob'
    });
    if (!options.logger) {
        options.logger = bunyan.createLogger({
            name: 'shard-' + options.namespace + '/' + options.jobName + '/' + options.shardInfo.index
        });
    }
    this.$logger = options.logger;
    this.shardInfo = options.shardInfo;
    this.$etcd = client.etcd;
    this.$client = client;
    this.$masterKey = path.join(options.namespace, 'jobs', options.jobName, 'shards', options.shardInfo.index.toString(), 'master');
    this.$statusKey = path.join(options.namespace, 'jobs', options.jobName, 'shards', options.shardInfo.index.toString(), 'status');
    this.$jobHeartbeatRootKey = path.join(options.namespace, 'jobs', options.jobName, 'clients');
    this.master = false;
    this.$waitTime = 0;
    this.$masterListener = () => {
        this.master = true;
        if (!this.$timer) {
            this.$timer = setInterval(() => {
                this.$etcd.ttl(this.$masterKey, 30).then((result) => {
                    this.$logger.debug('Master heartbeat.');
                    this.emit('ttl');
                }).catch((err) => {
                    this.$logger.error('Master heartbeat failed.', err);
                });
            }, 10000);
        }
    };
    this.$slaveListener = () => {
        this.master = false;
        if (this.$timer) {
            clearInterval(this.$timer);
            delete this.$timer;
            this.$logger.debug('Master heartbeat interval cleared.');
        }
        if (this.$executeTimer) {
            clearTimeout(this.$executeTimer);
            delete this.$executeTimer;
            this.$logger.debug('Execute timer cleared.');
        }
    };
    this.on('elect', () => {
        let wait = Math.floor(Math.random() * 100 + this.$waitTime);
        this.$logger.info('Election started, wait for %d millis', wait);
        setTimeout(() => {
            this.$logger.info('Try to elect master.');
            this.$etcd.set(this.$masterKey, this.$client.id, {
                prevExist: false,
                ttl: 30
            }).then(() => {
                this.$logger.info('This instance became master.');
                this.$masterListener();
                this.emit('master', this.shardInfo);
            }).catch((err) => {
                this.$logger.info('This instance became slave.');
                this.$slaveListener();
                this.emit('slave', this.shardInfo);
            });
        }, wait);
    });
}

/**
 *
 * @returns {Promise}
 */
Shard.prototype.start = function () {
    let index;
    let statusIndex;

    let gotStatus = (data) => {
        try {
            let status = JSON.parse(data.node.value);
            this.status = status.status;
            this.emit('status', this.status);
            if (this.status === 'running') {
                this.$logger.debug('Running shard detected, start a watcher to clear dead master.');
                this.$runningWatcherTimer = setInterval(() => {
                    this.$etcd.get(path.join(this.$runningWatcherTimer, status.master)).catch((err) => {
                        if (err.errorCode === 100) {
                            this.$logger.info('Master of a running shard dead, reset to ready.');
                            this.setStatus('ready', {
                                prevIndex: data.node.modifiedIndex
                            }).catch((err) => {
                                this.$logger.info('Status reseted by other process, ignore it.');
                            })
                        }
                    });
                }, 30000);
            } else {
                if (this.$runningWatcherTimer) {
                    this.$logger.debug('Running watcher timer exist, clear and delete it.');
                    clearInterval(this.$runningWatcherTimer);
                    delete this.$runningWatcherTimer;
                }
                if (this.$runningWatcher) {
                    this.$logger.debug('Running watcher exist, stop and delete it.');
                    this.$runningWatcher.stop().then(() => {
                        delete this.$runningWatcher;
                    });
                }
            }
        } catch (e) {
            this.emit('error', e);
        }
    };
    let promises = [
        //Init election and watcher
        this.$etcd.get(this.$masterKey).then((result) => {
            index = result.node.modifiedIndex;
            if (result.node.value === this.$client.id) {
                this.$logger.info('Got election result, this instance is master');
                this.$masterListener();
                this.emit('master', this.shardInfo);
            } else {
                this.$logger.info('Got election result, this instance is slave. master ttl is %d', result.node.ttl);
                this.$slaveListener();
                this.emit('slave', this.shardInfo);
            }
        }).catch((err) => {
            this.$logger.debug('Election status not exist, try to create it as master');
            return this.$etcd.set(this.$masterKey, this.$client.id, {
                prevExist: false,
                ttl: 30
            }).then((result) => {
                this.$logger.info('Election status created, this instance is master.');
                this.$masterListener();
                this.emit('master', this.shardInfo);
                index = result.node.modifiedIndex;
            }).catch((err) => {
                this.$logger.info('Election status created by other process, this instance is slave.');
                this.$slaveListener();
                this.emit('slave', this.shardInfo);
            })
        }).then(() => {
            this.$logger.info('Election status inited, watch it from index %d', index + 1);
            return this.$etcd.watcher(this.$masterKey, index + 1).then((watcher) => {
                this.$electionWatcher = watcher;
                watcher.on('error', (err) => {
                    console.log(err);
                });
                let elect = () => {
                    this.emit('elect');
                };
                watcher.on('expire', elect);
                watcher.on('delete', elect);
                watcher.start();
                this.$logger.debug('Election watcher created.');
            });
        }),
        //Init status and watcher
        this.$etcd.get(this.$statusKey).then((data) => {
            this.$logger.debug('Shard exists, get shard status directly');
            statusIndex = data.node.modifiedIndex;
            gotStatus(data);
        }).catch((err) => {
            this.$logger.debug('Shard does not exist, try to init status');
            return this.setStatus('ready', {
                prevExist: false
            }).then((data) => {
                this.$logger.debug('Shard status inited.');
                statusIndex = data.node.modifiedIndex;
                gotStatus(data);
            }).catch((err) => {
                this.$logger.debug('Shard status inited by other process, try to get it');
                return this.$etcd.get(this.$statusKey).then((data) => {
                    this.$logger.debug('Got status inited by other process');
                    statusIndex = data.node.modifiedIndex;
                    gotStatus(data);
                })
            })
        }).then(() => {
            this.$logger.info('Shard status inited, watch it from index %d', statusIndex + 1);
            return this.$etcd.watcher(this.$statusKey, statusIndex + 1).then((watcher) => {
                //Got status watcher
                this.$statusWatcher = watcher;
                watcher.on('change', gotStatus);
                watcher.start();
                this.$logger.debug('Status watcher created.');
            })
        })
    ];
    return Promise.all(promises).then(() => {
        this.emit('started', this.shardInfo);
    }).catch((err) => {
        this.emit('error', err);
    });
};

/**
 *
 * @returns {Promise}
 */
Shard.prototype.stop = function () {
    this.$logger.info('Stopping shard...');
    let promises = [
        this.$statusWatcher.stop().then(() => {
            delete this.$statusWatcher;
        }),
        this.$electionWatcher.stop().then(() => {
            delete this.$electionWatcher;
        }),
        Promise.method(() => {
            if (this.master) {
                return this.$etcd.remove(this.$masterKey).then(() => {
                    this.$logger.info('Master key deleted.');
                }).catch((err) => {
                    this.$logger.error('Error when deleting master key.', err);
                });
            }
        })().then(() => {
            this.$slaveListener();
        })
    ];
    return Promise.all(promises).then(() => {
        this.$logger.info('Shard stopped');
    }).catch((err) => {
        this.$logger.error('Shard stopped with error', err);
        throw err;
    });
};

Shard.prototype.setStatus = function (status, options) {
    let args = arguejs({
        status: String,
        options: [Object, {}]
    }, arguments);
    return this.$etcd.set(this.$statusKey, JSON.stringify({
        status: args.status,
        master: this.$client.id,
        timestamp: new Date().getTime()
    }), args.options);
};

/**
 * This will fail this shard and make the job into error state
 * @returns {Promise}
 */
Shard.prototype.fail = function () {
    return this.setStatus('failed');
};

/**
 * This will finish this shard
 * @returns {Promise}
 */
Shard.prototype.finish = function () {
    this.$logger.info('Executor finished.');
    return this.setStatus('completed');
};

Shard.prototype.schedule = function (executor, timeout) {
    if (this.master) {
        this.$logger.info('Scheduled executor to run after %d millis', timeout);
        this.$executeTimer = setTimeout(() => {
            this.setStatus('running').then(() => {
                try {
                    this.$logger.info('Executor start to run.');
                    let promise = executor(this.shardInfo);
                    if (promise && promise.then) {
                        return promise.then(() => {
                            return this.finish();
                        }).catch((err) => {
                            this.$logger.error('Executor finished with error', err);
                            return this.fail();
                        });
                    } else {
                        return this.finish();
                    }
                } catch (e) {
                    this.$logger.error('Executor finished with error', e);
                    return this.fail();
                }
            }).catch((err) => {
                this.$logger.error('Failed to change shard status to running.');
                this.emit('error', err);
            });
        }, timeout);
    } else {
        this.$logger.warn('Trying to schedule a executor on slave shard.');
    }
};

Shard.prototype.isFinished = function () {
    return this.status === 'completed' || this.status === 'ready';
};

Shard.prototype.isMaster = function () {
    return this.master;
};

util.inherits(Shard, EventEmitter);

module.exports = Shard;