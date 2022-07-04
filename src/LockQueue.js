'use strict';

class LockQueue {
    constructor(queueSize) {
        this.queueSize = queueSize;
        this.freed = true;
        this.waitingQueue = [];
    }

    //async
    get(take = true) {
        return new Promise((resolve, reject) => {
            if (this.freed) {
                if (take)
                    this.freed = false;
                resolve();
                return;
            }

            if (this.waitingQueue.length < this.queueSize) {
                this.waitingQueue.push(() => {
                    resolve();
                });
            } else {
                reject(new Error('Lock queue is too long'));
            }
        });
    }

    ret() {
        if (this.waitingQueue.length) {
            const onFreed = this.waitingQueue.shift();
            onFreed();
        } else {
            this.freed = true;
        }
    }

    //async
    wait() {
        return this.get(false);
    }

    free() {
        while (this.waitingQueue.length) {
            const onFreed = this.waitingQueue.shift();
            onFreed();
        }
        this.freed = true;
    }

}

module.exports = LockQueue;