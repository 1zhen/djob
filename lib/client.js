/**
 * Created by nicholas on 17-3-10.
 */
const Etcd = require('etcd-cli');
const uuid = require('uuid');

function Client(etcdHost) {
    this.id = uuid.v4();
    this.etcd = new Etcd.V2HTTPClient(etcdHost);
}

module.exports = Client;