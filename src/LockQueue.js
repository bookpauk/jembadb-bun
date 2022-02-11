'use strict';

class LockQueue {
    constructor(queueSize) {
        this.queueSize = queueSize;
        this.freed = true;
        this.waitingQueue = [];
    }

    get() {
        return new Promise((resolve) => {
            if (this.freed) {
                this.freed = false;
                resolve();
                return;
            }

            if (this.waitingQueue.length >= this.queueSize)
                throw new Error('Lock queue is too long');

            this.waitingQueue.push({
                onFreed: () => {
                    this.freed = false;
                    resolve();
                },
            });
        });
    }

    ret() {
        this.freed = true;
        if (this.waitingQueue.length) {
            this.waitingQueue.shift().onFreed();
        }
    }

    wait() {
        return new Promise((resolve) => {
            if (this.freed) {
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

    free() {
        while (this.waitingQueue.length) {
            this.waitingQueue.shift().onFreed();
        }
        this.freed = true;
    }

}

module.exports = LockQueue;