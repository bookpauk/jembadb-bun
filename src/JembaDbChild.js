'use strict';

const { parentPort } = require('worker_threads');

const JembaDb = require('./JembaDb');

const db = new JembaDb();

if (parentPort) {
    parentPort.on('message', async(mes) => {
        let result = {};
        try {
            if (db[mes.action])
                result.result = await db[mes.action](mes.query);
            else
                result = {error: 'Action not found: ' + mes.action};
        } catch (e) {
            result = {error: e.message};
        }

        result.requestId = mes.requestId;
        parentPort.postMessage(result);
    });
}
