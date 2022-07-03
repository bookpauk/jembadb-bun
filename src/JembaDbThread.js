'use strict';

const { Worker } = require('worker_threads');
const utils = require('./utils');
const JembaDbChild = require('./JembaDbChild');
/* API methods:
lock
unlock

create
drop
truncate
clone

open
openAll
close
closeAll

tableExists
getDbInfo
getDbSize
setMonitoring

select
insert
update
delete

markCorrupted

esc
*/

class JembaDbThread {
    constructor() {
        this.worker = null;
        this.listeners = new Map();
        this.requestId = 0;

        const apiMethods = [
            //lock, unlock,
            'create', 'drop', 'truncate', 'clone', 'open', 'openAll', 'close', 'closeAll',
            'tableExists', 'getDbInfo', 'getDbSize', 'setMonitoring',
            'select', 'insert', 'update', 'delete',
            'markCorrupted'
        ];

        for (const action of apiMethods) {
            this[action] = async(query) => this._action(action, query);
        }
    }

    _terminate() {
        if (this.worker) {
            for (const listener of this.listeners.values()) {
                listener({error: 'Worker terminated'});
            }
            this.worker.terminate();
        }
        this.worker = null;
    }

    _runWoker() {
        //const worker = new Worker(`${__dirname}/JembaDbChild.js`);
        const worker = new Worker(JembaDbChild, {eval: true});

        worker.on('message', (mes) => {
            const listener = this.listeners.get(mes.requestId);
            if (listener)
                listener(mes);
        });

        worker.on('error', (err) => {
            console.error(err);
        });

        worker.on('exit', () => {
            this._terminate();
        });

        this.worker = worker;
    }    

    _action(action, query) {
        return new Promise((resolve, reject) => {
            this.requestId++;

            const requestId = this.requestId; //!!!
            this.listeners.set(requestId, (mes) => {
                this.listeners.delete(requestId);

                if (mes.error)
                    reject(new Error(mes.error));
                else
                    resolve(mes.result);
            });

            if (this.worker) {
                this.worker.postMessage({requestId: this.requestId, action, query});
            } else {
                reject(new Error('Worker does not exist (database closed?)'));
            }
        });
    }

    async lock(query = {}) {
        if (!this.worker) {
            this._runWoker();
        } else {
            throw new Error('Worker has been created already');
        }

        try {
            await this._action('lock', query);
        } catch(e) {
            this._terminate();
            throw e;
        }
    }

    async unlock() {
        if (this.worker)
            await this._action('unlock');
        this._terminate();
        //console.log('DB closed');
    }

    esc(obj) {
        return utils.esc(obj);
    }
}

module.exports = JembaDbThread;