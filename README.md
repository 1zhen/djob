# djob
A simple distribute job framework for Node.JS

## Install
```shell
$ npm install djob
```

## Quick start
```javascript
const DJob = require('djob');
const bunyan = require('bunyan');

let djob = new DJob('127.0.0.1:2379');
let job = djob.createJob({
        namespace: 'djob',  //Optional, default is djob
        name: 'somejob',
        cron: '*/30 * * * * *',
        shards: 1           //Optional, defualt is 1,
        logger: bunyan.createLogger({name: 'somelogger'})   //Optional, any bunyan logger will be ok
    }, (shardInfo) => {
        // shardInfo = {
        //     index: 0     //Index of the shard which run the executor.
        // }
        //For async logics, you must return a Promise to make sure that the job status correctly.
        //Notice: All error in this function will set the job status to failed, and cannot auto recover.
    });

job.start().then(() => {
    //Do things depends the job to start
});

job.stop().then(() => {
    //Stop the job before exit
})
```