/**
 * Created by nicholas on 17-3-10.
 */
const Etcd = require('etcd-cli');
const uuid = require('uuid');
const Job = require('./job');

/**
 *
 * @param {string|Array} etcdHost
 * @constructor
 */
function Client(etcdHost) {
    this.id = uuid.v4();
    this.etcd = new Etcd.V2HTTPClient(etcdHost);
}

/**
 * @name ShardInfo
 * @property {int} index
 */

/**
 * @name Executor
 * @type function
 * @param {ShardInfo} shardInfo
 * @returns {Promise|undefined}
 */

/**
 * @name JobOptions
 * @property {string} [namespace]
 * @property {string} name
 * @property {string} cron
 * @property {int} [shards]
 */

/**
 *
 * @param {JobOptions} options
 * @param {Executor|function} executor
 * @returns {Job}
 */
Client.prototype.createJob = function (options, executor) {
    return new Job(this, options, executor);
};

module.exports = Client;