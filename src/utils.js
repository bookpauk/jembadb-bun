'use strict';

const mson = require('./mson');
const fsCB = require('fs');
const fs = fsCB.promises;
const zlib = require('zlib');

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function sleepWithStop(ms, cb = () => {}) {
    return new Promise(resolve => {
        const timer = setTimeout(resolve, ms);
        cb(() => { clearTimeout(timer); resolve(); });
    });
}

function unionSet(arrSet) {
    if (!arrSet.length)
        return new Set();

    let max = 0;
    let size = arrSet[0].size;
    for (let i = 1; i < arrSet.length; i++) {
        if (arrSet[i].size > size) {
            max = i;
            size = arrSet[i].size;
        }
    }

    const result = new Set(arrSet[max]);
    for (let i = 0; i < arrSet.length; i++) {
        if (i === max)
            continue;

        for (const elem of arrSet[i]) {
            result.add(elem);
        }
    }

    return result;
}

function intersectSet(arrSet) {
    if (!arrSet.length)
        return new Set();

    let min = 0;
    let size = arrSet[0].size;
    for (let i = 1; i < arrSet.length; i++) {
        if (arrSet[i].size < size) {
            min = i;
            size = arrSet[i].size;
        }
    }

    const result = new Set();
    for (const elem of arrSet[min]) {
        let inAll = true;
        for (let i = 0; i < arrSet.length; i++) {
            if (i === min)
                continue;
            if (!arrSet[i].has(elem)) {
                inAll = false;
                break;
            }
        }

        if (inAll)
            result.add(elem);
    }


    return result;
}

async function pathExists(path) {
    try {
        await fs.access(path);
        return true;
    } catch(e) {
        return false;
    }
}

function esc(obj) {
    return mson.encode(obj).replace(/@/g, '\\x40');
}

function paramToArray(param) {
    return (Array.isArray(param) ? param : [param]);
}

function cloneDeep(obj) {
    return mson.decode(mson.encode(obj));
}

//async
function deflate(buf, compressionLevel) {
    return new Promise((resolve, reject) => {
        zlib.deflateRaw(buf, {level: compressionLevel}, (err, b) => {
            if (err)
                reject(err);
            resolve(b);
        });
    });
}

//async
function inflate(buf) {
    return new Promise((resolve, reject) => {
        zlib.inflateRaw(buf, (err, b) => {
            if (err)
                reject(err);
            resolve(b);
        });
    });
}

function hasProp(obj, prop) {
    return Object.prototype.hasOwnProperty.call(obj, prop);
}

async function deleteFile(file) {
    if (await pathExists(file)) {
        await fs.unlink(file);
    }
}

// locking by file existence
// returns watcher
async function getFileLock(lockPath, softLock, ignoreLock) {
    const lockFile = `${lockPath}/__lock`;
    const softLockFile = `${lockPath}/__softlock`;
    const softLockCheckName = '__softlockcheck';
    const softLockCheckFile = `${lockPath}/${softLockCheckName}`;

    if (!await pathExists(lockPath)) {
        throw new Error(`Path does not exist: ${lockPath}`);
    }

    //check locks
    //  hard lock
    if (!ignoreLock && await pathExists(lockFile)) {
        throw new Error(`Path locked: ${lockPath}`);
    }

    //  soft lock
    if (!ignoreLock && await pathExists(softLockFile)) {
        let locked = true;

        await fs.writeFile(softLockCheckFile, '');

        let stat = null;
        try { stat = await fs.stat(softLockCheckFile); } catch(e) {} // eslint-disable-line no-empty

        await sleep(1000);//if main process is busy, give it time to delete softLockCheckFile

        if (await pathExists(softLockCheckFile)) {//not deleted
            let stat2 = null;
            try { stat2 = await fs.stat(softLockCheckFile); } catch(e) {} // eslint-disable-line no-empty
            if (stat && stat2 && stat.ctimeMs === stat2.ctimeMs) {//created by us
                locked = false;
            }
        }

        if (locked)
            throw new Error(`Path locked: ${lockPath}`);
    }

    await deleteFile(softLockCheckFile);
    await deleteFile(softLockFile);
    await deleteFile(lockFile);

    //obtain locks
    let fileWatcher;
    if (!softLock) {//hard lock
        await fs.writeFile(lockFile, '');
    } else {//soft lock
        await fs.writeFile(softLockFile, '');

        try {
            const watcher = fsCB.watch(lockPath, async(eventType, filename) => {
                if (filename == softLockCheckName) {
                    try {
                        await deleteFile(softLockCheckFile);
                    } catch (e) {
                        console.error(e);
                    }
                }
            });
            fileWatcher = {type: 'watcher', watcher};
        } catch (e) {//if watch not implemented
            fileWatcher = {type: 'timer', watching: true};
            (async() => {
                while (fileWatcher.watching) {
                    try {
                        await deleteFile(softLockCheckFile);
                    } catch (e) {
                        console.error(e);
                    }

                    await sleepWithStop(450, (stop) => {
                        fileWatcher.stop = stop;
                    });
                }
            })();
        }
    }

    return fileWatcher;
}

async function releaseFileLock(lockPath, fileWatcher) {
    const lockFile = `${lockPath}/__lock`;
    const softLockFile = `${lockPath}/__softlock`;
    const softLockCheckFile = `${lockPath}/__softlockcheck`;

    if (fileWatcher) {
        if (fileWatcher.type == 'watcher') {
            fileWatcher.watcher.close();
        } else if (fileWatcher.type == 'timer') {

            fileWatcher.watching = false;
            if (fileWatcher.stop)
                fileWatcher.stop();
        }
    }

    await deleteFile(softLockCheckFile);
    await deleteFile(softLockFile);
    await deleteFile(lockFile);
}

function freeMemory() {
    if (global.gc) {
        global.gc();
    }
}

module.exports = {
    sleep,
    sleepWithStop,
    unionSet,
    intersectSet,
    pathExists,
    esc,
    paramToArray,
    cloneDeep,
    deflate,
    inflate,
    hasProp,
    deleteFile,
    getFileLock,
    releaseFileLock,
    freeMemory,
};