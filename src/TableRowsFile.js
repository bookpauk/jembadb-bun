'use strict';

const fs = require('fs').promises;
const path = require('path');
const v8 = require('node:v8');

const utils = require('./utils');
const fileUtils = require('./fileUtils');
const LockQueue = require('./LockQueue');

const maxBlockSize = 1024*1024;//bytes

const minFileDumpSize = 100*1024;//bytes
const maxFileDumpSize = 50*1024*1024;//bytes

const unloadBlocksPeriod = 1000;//ms

class TableRowsFile {
    constructor(tablePath, cacheSize, compressed) {
        this.tablePath = tablePath;
        this.loadedBlocksCount = cacheSize || 5;
        this.loadedBlocksCount = (this.loadedBlocksCount <= 0 ? 0 : this.loadedBlocksCount);
        this.compressed = compressed || 0;

        this.fileLockMap = new Map();
        this.blockIndex = new Map();
        this.currentBlockIndex = 0;
        this.lastSavedBlockIndex = 0;
        this.blockList = new Map();
        this.blockSetDefrag = new Set();
        this.blocksNotFinalized = new Set();//indexes of blocks
        this.loadedBlocks = [];
        this.newBlocks = [];
        this.deltas = new Map();

        this.destroyed = false;

        this.blockindex0Size = 0;
        this.blocklist0Size = 0;

        this.fd = {
            blockIndex: null,
            blockList: null,
            blockRows: null,
            blockRowsIndex: null,//not a file descriptor
        };
    }

    //--- rows interface
    hasRow(id) {
        return this.blockIndex.has(id);
    }

    async getRow(id) {
        const block = this.blockList.get(this.blockIndex.get(id));

        if (!block) {
            return;
        }

        if (block.rows) {
            return block.rows.get(id);
        } else {
            await this.loadBlock(block);
            const result = block.rows.get(id);
            this.unloadBlocksIfNeeded();
            return result;
        }
    }

    setRow(id, row, rowStr, deltaStep) {
        const delta = this.getDelta(deltaStep);

        if (this.blockIndex.has(id)) {
            this.deleteRow(id, deltaStep, delta);
        }

        const index = this.addToCurrentBlock(id, row, rowStr, deltaStep, delta);        
        this.blockIndex.set(id, index);
        delta.blockIndex.push([id, index]);
    }

    deleteRow(id, deltaStep, delta) {
        if (this.blockIndex.has(id)) {
            if (!delta)
                delta = this.getDelta(deltaStep);

            const block = this.blockList.get(this.blockIndex.get(id));
            if (block) {
                block.delCount++;
                this.blockSetDefrag.add(block.index);
                delta.blockList.push([block.index, 1]);
            }

            this.blockIndex.delete(id);
            delta.blockIndex.push([id, 0]);
        }
    }

    getAllIds() {
        return this.blockIndex.keys();
    }

    getAllIdsSize() {
        return this.blockIndex.size;
    }
    //--- rows interface end

    getDelta(deltaStep) {
        if (this.deltas.has(deltaStep)) {
            return this.deltas.get(deltaStep);
        } else {
            const delta = {
                blockIndex: [],
                blockList: [],
                blockRows: [],
            };
            this.deltas.set(deltaStep, delta);
            return delta;
        }
    }

    getFileLock(fileName) {
        let queue = this.fileLockMap.get(fileName);
        
        if (!queue) {
            queue = new LockQueue(1000);
            this.fileLockMap.set(fileName, queue);
        }

        return queue;
    }

    metaBlock(block) {
        const result = Object.assign(
            {
                index: null,
                delCount: 0,
                addCount: 0,
                rows: null,//!!!
                size: 0,
                rowsLength: 0,
                final: false,
            },
            block
        );

        result.rows = null;//!!!
        return result;
    }


    createNewBlock() {
        this.currentBlockIndex++;
        const block = this.metaBlock({index: this.currentBlockIndex});
        block.rows = new Map();
        this.blockList.set(this.currentBlockIndex, block);
        this.newBlocks.push(this.currentBlockIndex);
        this.blocksNotFinalized.add(this.currentBlockIndex);

        return block;
    }

    addToCurrentBlock(id, row, rowSer, deltaStep, delta) {
        if (!delta)
            delta = this.getDelta(deltaStep);

        let block = this.blockList.get(this.currentBlockIndex);
        if (!block)
            block = this.createNewBlock();

        if (block.size > maxBlockSize)
            block = this.createNewBlock();

        if (!block.rows) {
            throw new Error('TableRowsFile: something has gone wrong');
        }

        block.rows.set(id, row);

        block.addCount++;
        block.size += rowSer.length + 4;
        block.rowsLength = block.rows.size;

        delta.blockList.push([block.index, 1]);
        delta.blockRows.push([block.index, rowSer]);

        return block.index;
    }

    unloadBlocksIfNeeded(fromTimer = false) {
        if (!fromTimer) {
            if (this.unloadTimer)
                return;

            this.unloadTimer = setTimeout(() => {this.unloadBlocksIfNeeded(true)}, unloadBlocksPeriod);
            return;
        }

        this.unloadTimer = null;

        try {
            const nb = [];
            for (const index of this.newBlocks) {
                if (index < this.lastSavedBlockIndex) {
                    this.loadedBlocks.push(index);
                } else {
                    nb.push(index);
                }
            }

            this.newBlocks = nb;

            if (this.loadedBlocks.length <= this.loadedBlocksCount)
                return;

            //check loaded
            while (this.loadedBlocks.length > this.loadedBlocksCount) {
                const index = this.loadedBlocks.shift();

                //additional check, just in case
                if (index >= this.lastSavedBlockIndex)
                    continue;

                const block = this.blockList.get(index);

                if (block) {
                    block.rows = null;
//console.log(this.loadedBlocks.length, this.loadedBlocksCount);
//console.log(`unloaded block ${block.index}`);
                }
            }
        } catch (e) {
            console.error(e);
        }
    }

    async loadBlock(block) {
//console.log(`start load block ${block.index}`);
        const fileName = this.blockRowsFilePath(block.index);
        const fLock = this.getFileLock(fileName);
        await fLock.get();
        try {
            if (!block.rows) {
                const arr = await fileUtils.loadFile(fileName, this.allowCorrupted);

                block.rows = new Map(arr);

                this.loadedBlocks.push(block.index);
//console.log(`loaded block ${block.index}`, this.lastSavedBlockIndex, this.currentBlockIndex);
            }
        } finally {
            fLock.ret();
        }
    }

    async closeFd(name) {
        if (this.fd[name]) {
            await this.fd[name].close();
            this.fd[name] = null;
        }
    }
    
    async openFd(name, fileName = '') {
        if (this.fd[name])
            return;

        if (!fileName) {
            throw new Error('TableRowsFile: fileName is empty');
        }

        this.fd[name] = await fileUtils.openFile(fileName);
    }
    
    blockRowsFilePath(index) {
        if (index < 1000000)
            return `${this.tablePath}/${index.toString().padStart(6, '0')}.jem`;
        else
            return `${this.tablePath}/${index.toString().padStart(12, '0')}.jem`;
    }

    async finalizeBlocks() {
//console.log(this.blocksNotFinalized.size);

        for (const index of this.blocksNotFinalized) {
            if (this.destroyed)
                return;

            if (index >= this.lastSavedBlockIndex)
                continue;

            const block = this.blockList.get(index);

            if (block) {
                if (block.final)
                    throw new Error('finalizeBlocks: something wrong');

                const blockPath = this.blockRowsFilePath(block.index);
//console.log(`start finalize block ${block.index}`);
                const arr = await fileUtils.loadFile(blockPath, this.allowCorrupted);
                const rows = new Map(arr);//!!! compressing appended key-values, the last one key-value pair is actual

                const finBlockPath = `${blockPath}.tmp`;
                const blockSize = await fileUtils.writeFinal(finBlockPath, Array.from(rows), this.compressed);

                await fs.rename(finBlockPath, blockPath);

                block.size = blockSize;
                block.rowsLength = rows.size;//insurance
                block.final = true;
                await fileUtils.appendRecs(this.fd.blockList, [v8.serialize(this.metaBlock(block))]);
//console.log(`finalized block ${block.index}`);
            }

            this.blocksNotFinalized.delete(index);
            this.blockSetDefrag.add(index);
        }
    }

    async dumpMaps() {
        //dumping blockIndex
        const blockindex1Size = (await this.fd.blockIndex.stat()).size;
        if ((blockindex1Size > minFileDumpSize && blockindex1Size > this.blockindex0Size) || blockindex1Size > maxFileDumpSize) {
            const blockindex0Path = `${this.tablePath}/blockindex.0`;
            const blockindex2Path = `${this.tablePath}/blockindex.2`;
            await fileUtils.writeFinal(blockindex2Path, Array.from(this.blockIndex), this.compressed);

            await fs.rename(blockindex2Path, blockindex0Path);
            await this.closeFd('blockIndex');
            await fs.unlink(`${this.tablePath}/blockindex.1`);
            this.blockindex0Size = (await fs.stat(blockindex0Path)).size;
        }

        //dumping blockList
        const blocklist1Size = (await this.fd.blockList.stat()).size;
        if ((blocklist1Size > minFileDumpSize && blocklist1Size > this.blocklist0Size) || blocklist1Size > maxFileDumpSize) {
            const blocklist0Path = `${this.tablePath}/blocklist.0`;
            const blocklist2Path = `${this.tablePath}/blocklist.2`;

            const metaBlocks = Array.from(this.blockList.values()).map(block => this.metaBlock(block));
            await fileUtils.writeFinal(blocklist2Path, metaBlocks, this.compressed);

            await fs.rename(blocklist2Path, blocklist0Path);
            await this.closeFd('blockList');
            await fs.unlink(`${this.tablePath}/blocklist.1`);
            this.blocklist0Size = (await fs.stat(blocklist0Path)).size;
        }
    }

    async saveDelta(deltaStep) {
        const delta = this.getDelta(deltaStep);

        //bug fix: this code must be exactly here due to defragmetation delta changes
        //lastSavedBlockIndex
        let lastSavedBI = 0;
        const len = delta.blockRows.length;
        if (len) {
            lastSavedBI = delta.blockRows[len - 1][0];
        }

        //check all blocks fragmentation & defragment if needed
        if (!this.defragCandidates)
            this.defragCandidates = [];

        if (!this.defragCandidates.length && this.blockSetDefrag.size) {
            for (const index of this.blockSetDefrag) {
                const block = this.blockList.get(index);
                if (!block || !block.final)
                    continue;

                if ( (block.delCount > 0 && block.addCount - block.delCount < block.rowsLength*0.6)
                    || block.size < maxBlockSize/2
                    ) {
                    this.defragCandidates.push(block);
                }
            }

            this.blockSetDefrag = new Set();
        }

        while (this.defragCandidates.length) {
            if (this.destroyed)
                break;

            const block = this.defragCandidates.shift();

            if (!block.rows) {
                await this.loadBlock(block);
            }

            //move all active rows from fragmented block to current
            for (const [id, row] of block.rows.entries()) {
                if (this.blockIndex.get(id) === block.index) {
                    const newIndex = this.addToCurrentBlock(id, row, v8.serialize([row.id, row]), deltaStep, delta);
                    this.blockIndex.set(id, newIndex);
                    delta.blockIndex.push([id, newIndex]);
                }
            }

            this.blockList.delete(block.index);
            delta.blockList.push([block.index, 0]);
            
            if (!delta.delFiles)
                delta.delFiles = [];
            delta.delFiles.push(this.blockRowsFilePath(block.index));

//console.log(`defragmented block ${block.index}, size: ${block.size}, addCount: ${block.addCount}, delCount: ${block.delCount}, rowsLength: ${block.rowsLength}`);
        }

        //blockIndex delta save
        if (!this.fd.blockIndex)
            await this.openFd('blockIndex', `${this.tablePath}/blockindex.1`);

        let buf = [];
        for (const deltaRec of delta.blockIndex) {
            buf.push(v8.serialize(deltaRec));
        }
        if (buf.length)
            await fileUtils.appendRecs(this.fd.blockIndex, buf);

        //blockList delta save
        if (!this.fd.blockList)
            await this.openFd('blockList', `${this.tablePath}/blocklist.1`);

        let lastSaved = 0;
        buf = [];
        for (const deltaRec of delta.blockList) {
            const index = deltaRec[0];
            const exists = deltaRec[1];
            
            if (exists) {
                if (lastSaved !== index) {//optimization
                    const block = this.blockList.get(index);
                    if (block)//might be defragmented already
                        buf.push(v8.serialize(this.metaBlock(block)));
                    lastSaved = index;
                }
            } else {
                buf.push(v8.serialize({index, deleted: 1}));
            }
        }
        if (buf.length)
            await fileUtils.appendRecs(this.fd.blockList, buf);

        //blockRows delta save
        buf = [];
        for (const deltaRec of delta.blockRows) {
            const [index, rowSer] = deltaRec;

            if (this.fd.blockRowsIndex !== index) {
                if (buf.length)
                    await fileUtils.appendRecs(this.fd.blockRows, buf);
                buf = [];
                await this.closeFd('blockRows');
                this.fd.blockRowsIndex = null;
            }
        
            if (!this.fd.blockRows) {
                const blockPath = this.blockRowsFilePath(index);

                await this.openFd('blockRows', blockPath);
                this.fd.blockRowsIndex = index;
            }

            buf.push(rowSer);
        }
        if (buf.length)
            await fileUtils.appendRecs(this.fd.blockRows, buf);

        //lastSavedBlockIndex
        if (lastSavedBI) {
            this.lastSavedBlockIndex = lastSavedBI;
        }

        //blocks finalization
        await this.finalizeBlocks();
        this.unloadBlocksIfNeeded();

        //dumps if needed
        await this.dumpMaps();

        //delete files if needed
        if (delta.delFiles) {
            for (const fileName of delta.delFiles) {
//console.log(`delete ${fileName}`);                
                const fLock = this.getFileLock(fileName);
                await fLock.get();
                try {
                    if (await utils.pathExists(fileName))
                        await fs.unlink(fileName);
                    this.fileLockMap.delete(fileName);
                } finally {
                    fLock.ret();
                }
            }
        }

        this.deltas.delete(deltaStep);
    }

    async cancelDelta(deltaStep) {
        this.deltas.delete(deltaStep);
    }

    async load() {
        let autoIncrement = 0;

        const loadBlockIndex = (fileNum, data) => {
            if (fileNum === 0) {//dumped data
                this.blockIndex = new Map(data);//much faster
                for (const id of this.blockIndex.keys()) {
                    if (typeof(id) === 'number' && id >= autoIncrement)
                        autoIncrement = id + 1;
                }
            } else {
                for (const rec of data) {
                    const [id, index] = rec;
                    if (index > 0) {
                        this.blockIndex.set(id, index);
                        if (typeof(id) === 'number' && id >= autoIncrement)
                            autoIncrement = id + 1;
                    } else
                        this.blockIndex.delete(id);
                }
            }
        }

        const loadBlockList = (data) => {
            for (const rec of data) {
                const block = rec;
                if (block.deleted) {
                    this.blockList.delete(block.index);
                } else {
                    block.rows = null;
                    this.blockList.set(block.index, block);
                    if (block.index > this.currentBlockIndex)
                        this.currentBlockIndex = block.index;
                }
            }

        }

        this.blockIndex.clear();
        for (let i = 0; i < 2; i++) {
            const dataPath = `${this.tablePath}/blockindex.${i}`;

            if (await utils.pathExists(dataPath)) {
                const data = await fileUtils.loadFile(dataPath, this.allowCorrupted);
                loadBlockIndex(i, data);
            }
        }
        const blockindex0Path = `${this.tablePath}/blockindex.0`;
        if (await utils.pathExists(blockindex0Path))
            this.blockindex0Size = (await fs.stat(blockindex0Path)).size;

        this.currentBlockIndex = 0;
        this.blockList.clear();
        for (let i = 0; i < 2; i++) {
            const dataPath = `${this.tablePath}/blocklist.${i}`;

            if (await utils.pathExists(dataPath)) {
                const data = await fileUtils.loadFile(dataPath, this.allowCorrupted);
                loadBlockList(data);
            }
        }
        const blocklist0Path = `${this.tablePath}/blocklist.0`;
        if (await utils.pathExists(blocklist0Path))
            this.blocklist0Size = (await fs.stat(blocklist0Path)).size;

        this.lastSavedBlockIndex = this.currentBlockIndex;
        const currentBlock = this.blockList.get(this.currentBlockIndex);
        if (currentBlock) {
            await this.loadBlock(currentBlock);
            this.newBlocks.push(this.currentBlockIndex);
            this.loadedBlocks = [];
        }

        this.blocksNotFinalized = new Set();
        for (const block of this.blockList.values()) {
            this.blockSetDefrag.add(block.index);
            if (!block.final)
                this.blocksNotFinalized.add(block.index);
        }

        return autoIncrement;
    }

    async loadCorrupted() {
        this.allowCorrupted = true;

        const loadBlockIndex = (fileNum, data) => {
            if (fileNum === 0) {//dumped data
                this.blockIndex = new Map(data);//much faster
            } else {
                for (const rec of data) {
                    const [id, index] = rec;
                    if (index > 0)
                        this.blockIndex.set(id, index);
                    else
                        this.blockIndex.delete(id);
                }
            }
        }

        this.blockIndex.clear();
        for (let i = 0; i < 2; i++) {
            const dataPath = `${this.tablePath}/blockindex.${i}`;

            if (await utils.pathExists(dataPath)) {
                try {
                    const data = await fileUtils.loadFile(dataPath, this.allowCorrupted);
                    loadBlockIndex(i, data);
                } catch(e) {
                    console.error(e);
                }
            }
        }

        const files = await fs.readdir(this.tablePath, { withFileTypes: true });

        this.blockList.clear();
        for (const file of files) {
            if (file.isFile() && path.extname(file.name) == '.jem') {
                const numStr = path.basename(file.name, '.jem');
                const index = parseInt(numStr, 10);
                if (!isNaN(index)) {
                    const block = this.metaBlock({index});
                    this.blockList.set(block.index, block);
                    //console.log(index);
                }
            }
        }
    }

    async closeAllFiles() {
        await this.closeFd('blockIndex');
        await this.closeFd('blockList');
        await this.closeFd('blockRows');
    }

    async destroy() {
        await this.closeAllFiles();

        if (this.unloadTimer) {
            clearTimeout(this.unloadTimer);
            this.unloadTimer = null;
        }

        this.destroyed = true;
    }
}

module.exports = TableRowsFile;