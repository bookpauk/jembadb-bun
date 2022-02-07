'use strict';

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
    return JSON.stringify(obj).replace(/@/g, '\\x40');
}

function paramToArray(param) {
    return (Array.isArray(param) ? param : [param]);
}

function cloneDeep(obj) {
    return JSON.parse(JSON.stringify(obj));
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
// returns timer
async function getFileLock(lockPath, softLock, ignoreLock) {
    const lockFile = `${lockPath}/__lock`;
    const softLockFile = `${lockPath}/__softlock`;
    const softLockCheckFile = `${lockPath}/__softlockcheck`;

    //check locks
    //  hard lock
    if (!ignoreLock && await pathExists(lockFile)) {
        throw new Error(`Path locked: ${lockPath}`);
    }

    //  soft lock
    if (!ignoreLock && await pathExists(softLockFile)) {
        await fs.writeFile(softLockCheckFile, '');
        const stat = await fs.stat(softLockCheckFile);
        await sleep(1000);
        let locked = true;
        if (await pathExists(softLockCheckFile)) {//not deleted
            const stat2 = await fs.stat(softLockCheckFile);
            if (stat.ctimeMs === stat2.ctimeMs) {//created by us
                locked = false;
            }
        }

        if (locked)
            throw new Error(`Path locked: ${lockPath}`);
    }

    await deleteFile(softLockCheckFile);
    await deleteFile(softLockFile);
    await deleteFile(lockFile);

    //get locks
    let timer;
    if (!softLock) {//hard lock
        await fs.writeFile(lockFile, '');
    } else {//soft lock
        await fs.writeFile(softLockFile, '');

        timer = setInterval(async() => {
            await deleteFile(softLockCheckFile);
        }, 200);
    }

    return timer;
}

async function releaseFileLock(lockPath, timer) {
    const lockFile = `${lockPath}/__lock`;
    const softLockFile = `${lockPath}/__softlock`;
    const softLockCheckFile = `${lockPath}/__softlockcheck`;

    if (timer)
        clearInterval(timer);

    await deleteFile(softLockCheckFile);
    await deleteFile(softLockFile);
    await deleteFile(lockFile);
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
};