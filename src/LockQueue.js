'use strict';

class LockQueue {
    constructor(queueSize) {
        this.queueSize = queueSize;
        this.freed = true;
        this.waitingQueue = [];
    }

    //async
    get(take = true) {
        return new Promise((resolve) => {
            if (this.freed) {
                if (take)
                    this.freed = false;
                resolve();
                return;
            }

            if (this.waitingQueue.length >= this.queueSize)
                throw new Error('Lock queue is too long');

            this.waitingQueue.push({
                onFreed: () => {
                    resolve();
                },
            });
        });
    }

    ret() {
        if (this.waitingQueue.length) {
            this.waitingQueue.shift().onFreed();
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
            this.waitingQueue.shift().onFreed();
        }
        this.freed = true;
    }

}

module.exports = LockQueue;