'use strict';
/*
    Total maximum rec count is unlimited.

    Limitations:
    - no rec.id while insert
    - rec.shard field required while insert
    - no unique hashes and indexes
    - maximum rec count per one shard is ~16000000 (limitation of JS Map)
    - seeming unexpected behavior for select|update|delete with sort,limit,offset params, but it is not
*/
const fs = require('fs').promises;
const utils = require('./utils');
const LockQueue = require('./LockQueue');

const BasicTable = require('./BasicTable');

const shardRowCountStep = 20*1000*1000;//must be greater than 16M, do not change
const maxFreeShardNumsLength = 100;
const maxAutoShardListLength = 1000;
const autoShardName = '___auto';

class ShardedTable {
    constructor() {
        this.type = 'sharded';

        this.autoIncrement = 0;
        this.fileError = '';

        this.shardLockMap = new Map();
        this.duiLockMap = new Map();
        this.cachedShardLock = new LockQueue(100);

        this.metaTable = null; //basic table
        this.infoShard = {id: '', count: 0};

        /*
        {
            id: String,//shard
            num: Number,
            count: Number,
        }
        */
        this.shardList = new Map();
        this.shardListTable = null;//basic table

        this.openedShardLockList = new Map;//{lock: Number, pers: Number}

        this.openedShardTables = new Map();//basic tables
        this.closableShardNames = new Set();
        this.cachedShardNames = new Set();

        this.freeShardNums = [];

        this.autoShard = {
            step: 0,
            list: [],//{shard: String, count: Number}
        };

        this.opened = false;
        this.closed = false;
        this.changedTables = [];

        //table open query
        this.openQuery = {};

        //table options defaults
        this.cacheShards = 1;
        this.autoShardSize = 1000000;
    }

    _checkErrors() {
        if (this.fileError)
            throw new Error(this.fileError);

        if (this.closed)
            throw new Error('Table closed');

        if (!this.opened)
            throw new Error('Table has not been opened yet');
    }

    async _cloneTable(srcTablePath, destTablePath, cloneSelf = false, noMeta = false, filter) {
        await fs.rmdir(destTablePath, { recursive: true });
        await fs.mkdir(destTablePath, { recursive: true });

        let srcShardListTable = null;
        let srcMetaTable = null;

        if (cloneSelf) {
            srcShardListTable = this.shardListTable;
            srcMetaTable = this.metaTable;
        } else {
            srcShardListTable = new BasicTable();
            await srcShardListTable.open({tablePath: `${srcTablePath}/shards`, autoRepair: this.autoRepair});
            srcMetaTable = new BasicTable();
            await srcMetaTable.open({tablePath: `${srcTablePath}/meta`, autoRepair: this.autoRepair});
        }

        const meta = await srcMetaTable.getMeta();
        if (!cloneSelf)
            await srcMetaTable.close();

        const srcShardList = await srcShardListTable.select({});
        if (!cloneSelf)
            await srcShardListTable.close();

        const destShardListTable = new BasicTable();
        await destShardListTable.open({tablePath: `${destTablePath}/shards`});

        const destMetaTable = new BasicTable();
        await destMetaTable.open({tablePath: `${destTablePath}/meta`});
        if (!noMeta)
            await destMetaTable.create(meta);
        await destMetaTable.close();

        const query = utils.cloneDeep(this.openQuery);
        const nodata = (filter === 'nodata');

        let totalCount = 0;
        if (!nodata) {
            for (const shardRec of srcShardList) {
                if (shardRec.id === '')
                    continue;
                
                const toTablePath = this._shardTablePath(shardRec.num, destTablePath);

                //cloning shard
                if (cloneSelf) {
                    const table = await this._lockShard(shardRec.id);
                    try {
                        await table.clone({toTablePath, noMeta: true, filter});
                    } finally {
                        await this._unlockShard(shardRec.id);
                    }
                } else {
                    const table = new BasicTable();
                    query.tablePath = this._shardTablePath(shardRec.num, srcTablePath);
                    await table.open(query);

                    await table.clone({toTablePath, noMeta: true, filter});
                    await table.close();
                }

                //read cloned shard, create meta
                const destTable = new BasicTable();
                query.tablePath = toTablePath;

                await destTable.open(query);
                if (!noMeta)
                    await destTable.create(meta);

                const shardCount = await destTable.select({count: true});
                await destTable.close();

                //save shardRec
                const count = shardCount[0].count;
                if (count > 0) {
                    await destShardListTable.insert({rows: [{id: shardRec.id, num: shardRec.num, count}]});
                    totalCount += count;
                } else {
                    await fs.rmdir(toTablePath, { recursive: true });
                }
            }
        }

        await destShardListTable.insert({rows: [{id: '', count: totalCount}]});
        await destShardListTable.close();

        await fs.writeFile(`${destTablePath}/state`, '1');
        await fs.writeFile(`${destTablePath}/type`, this.type);
    }

    async _recreateTable() {
        const tempTablePath = `${this.tablePath}___temporary_recreating`;

        await this._cloneTable(this.tablePath, tempTablePath);

        await fs.rmdir(this.tablePath, { recursive: true });
        await fs.rename(tempTablePath, this.tablePath);
    }

    async _loadMeta() {
        this.metaTable = new BasicTable();
        await this.metaTable.open({tablePath: `${this.tablePath}/meta`, autoRepair: this.autoRepair});

        this.shardListTable = new BasicTable();
        await this.shardListTable.open({tablePath: `${this.tablePath}/shards`, autoRepair: this.autoRepair});

        const rows = await this.shardListTable.select({});//all
        for (const row of rows) {
            if (row.id !== '') {
                this.shardList.set(row.id, row);
            } else {
                this.infoShard = row;
            }
        }
    }

    async _saveShardRec(shardRec) {
        await this.shardListTable.insert({rows: [shardRec], replace: true});
        this.changedTables.push(this.shardListTable);
        this._checkTables(); //no await
    }

    async _delShardRec(shardRec) {
        await this.shardListTable.delete({where: `@@id(${utils.esc(shardRec.id)})`});
        this.changedTables.push(this.shardListTable);
        this._checkTables(); //no await
    }

    _shardTablePath(num, path = '') {
        if (num < 1000000)
            return `${(path ? path : this.tablePath)}/s${num.toString().padStart(6, '0')}`;
        else
            return `${(path ? path : this.tablePath)}/s${num.toString().padStart(12, '0')}`;
    }

    _getFreeShardNum() {
        if (!this.freeShardNums.length) {
            this.freeShardNums = [];
            const nums = new Set();
            for (const shardRec of this.shardList.values()) {
                nums.add(shardRec.num);
            }

            let i = 0;
            while (this.freeShardNums.length < maxFreeShardNumsLength) {
                if (!nums.has(i))
                    this.freeShardNums.push(i);
                i++;
            }
        }

        return this.freeShardNums.shift();
    }

    _getShardLock(table) {
        let queue = this.shardLockMap.get(table);
        
        if (!queue) {
            queue = new LockQueue(100);
            this.shardLockMap.set(table, queue);
        }

        return queue;
    }

    _getDUILock(table) {
        let queue = this.duiLockMap.get(table);
        
        if (!queue) {
            queue = new LockQueue(100);
            this.duiLockMap.set(table, queue);
        }

        return queue;
    }

    _checkCachedShardLock() {
        if (this.cachedShardNames.size <= this.cacheShards) {
            this.cachedShardLock.ret();
            return;
        }

        if (this.cachedShardLock.freed)
            this.cachedShardLock.get();
    }

    _updateOpenedShardLockList(shard, lockN = 0, persN = 0) {
        let lockRec = this.openedShardLockList.get(shard);

        if (!lockRec) {
            lockRec = {lock: 0, pers: 0};
            this.openedShardLockList.set(shard, lockRec);
        }

        lockRec.lock += lockN;
        if (lockRec.lock < 0) lockRec.lock = 0;
        
        lockRec.pers += persN;
        if (lockRec.pers < 0) lockRec.pers = 0;

        if (lockRec.pers > 0) {
            this.closableShardNames.delete(shard);
            this.cachedShardNames.delete(shard);
            this._checkCachedShardLock();
        } else {
            if (lockRec.lock > 0) {
                this.closableShardNames.delete(shard);
            } else {
                this.closableShardNames.add(shard);
            }
            this.cachedShardNames.add(shard);
            this._checkCachedShardLock();
        }

    }

    async _closeShards(closeAll = false) {
        if (closeAll) {
            for (const [shard, table] of this.openedShardTables) {
                await table.close();
                this.openedShardTables.delete(shard);
            }
        } else {
            if (this.cachedShardNames.size <= this.cacheShards)
                return;

            for (const shard of this.closableShardNames) {
                const shdLock = this._getShardLock(shard);

                await shdLock.get();
                try {
                    if (this.cachedShardNames.size > this.cacheShards) {
                        if (this.closableShardNames.has(shard) && this.openedShardTables.has(shard)) {
                            const table = this.openedShardTables.get(shard);

                            await table.close();

                            this.openedShardTables.delete(shard);
                            this.cachedShardNames.delete(shard);
                            this._checkCachedShardLock();
                        }
                        this.closableShardNames.delete(shard);
                    } else {
                        break;
                    }
                } finally {
                    shdLock.ret();
                }
            }
        }
    }

    async _delShards(shardArr) {
        this._checkErrors();

        for (const shard of shardArr) {
            const duiLock = this._getDUILock(shard);
            const shdLock = this._getShardLock(shard);

            await duiLock.get();
            await shdLock.get();
            try {
                if (this.openedShardTables.has(shard)) {
                    if (!this.closableShardNames.has(shard))
                        continue;

                    const table = this.openedShardTables.get(shard);

                    await table.close();

                    this.openedShardTables.delete(shard);
                    this.closableShardNames.delete(shard);
                    this.cachedShardNames.delete(shard);
                    this._checkCachedShardLock();
                }

                const shardRec = this.shardList.get(shard);
                if (!shardRec || shardRec.count > 0)
                    continue;

                await this._delShardRec(shardRec);
                this.shardList.delete(shard);
                this.openedShardLockList.delete(shard);

                await fs.rmdir(this._shardTablePath(shardRec.num), { recursive: true });
            } finally {
                shdLock.ret();
                duiLock.ret();
            }
        }
    }

    async _lockShard(shard) {
        this._checkErrors();

        const shdLock = this._getShardLock(shard);

        await shdLock.get();
        try {
            if (this.openedShardTables.has(shard)) {
                this._updateOpenedShardLockList(shard, 1, 0);
                return this.openedShardTables.get(shard);
            }

            await this.cachedShardLock.wait();

            let shardRec = this.shardList.get(shard);
            let isNew = !shardRec;
            if (isNew) {
                shardRec = {
                    id: shard,
                    num: this._getFreeShardNum(),
                    count: 0,
                };
            }

            const table = new BasicTable();

            const query = utils.cloneDeep(this.openQuery);
            query.tablePath = this._shardTablePath(shardRec.num);

            if (isNew) {
                await fs.rmdir(query.tablePath, { recursive: true });

                await table.open(query);

                table.autoIncrement = shardRowCountStep*shardRec.num;

                const meta = await this.metaTable.getMeta();
                await table.create(meta);

                this.shardList.set(shard, shardRec);
                await this._saveShardRec(shardRec);
            } else {
                await table.open(query);
            }

            this.openedShardTables.set(shard, table);
            this._updateOpenedShardLockList(shard, 1, 0);

            return table;
        } finally {
            shdLock.ret();
        }
    }

    async _unlockShard(shard) {
        this._updateOpenedShardLockList(shard, -1, 0);

        await this._closeShards();
    }

    /*
    query: {
        tablePath: String,
        cacheSize: Number,
        cacheShards: Number, 1, for sharded table only
        autoShardSize: Number, 1000000, for sharded table only
        compressed: Number, 0..9
        recreate: Boolean, false,
        autoRepair: Boolean, false,
        forceFileClosing: Boolean, false,
        typeCompatMode: Boolean, false,
    }
    */
    async open(query = {}) {
        if (this.opening)
            throw new Error('Table open in progress');

        this.opening = true;
        //console.log(query);
        try {
            if (this.opened)
                throw new Error('Table has already been opened');
            if (this.closed)
                throw new Error('Table instance has been destroyed. Please create a new one.');

            //opening
            if (!query.tablePath)
                throw new Error(`'query.tablePath' parameter is required`);

            this.tablePath = query.tablePath;
            this.cacheShards = query.cacheShards || this.cacheShards;
            this.autoShardSize = query.autoShardSize || this.autoShardSize;

            this.recreate = query.recreate || false;
            this.autoRepair = query.autoRepair || false;

            this.openQuery = utils.cloneDeep(query);
            this.openQuery.recreate = false;

            let create = true;
            if (await utils.pathExists(this.tablePath)) {
                create = false;
            } else {
                await fs.mkdir(this.tablePath, { recursive: true });
            }

            //check table version
            const statePath = `${this.tablePath}/state`;
            const typePath = `${this.tablePath}/type`;
            if (create) {
                await fs.writeFile(typePath, this.type);
                await fs.writeFile(statePath, '1');
            } else {
                let type = null;
                if (await utils.pathExists(typePath)) {
                    type = await fs.readFile(typePath, 'utf8');
                    if (type !== this.type)
                        throw new Error(`Wrong table type '${type}', expected '${this.type}'`);
                } else {
                    if (query.typeCompatMode) {
                        await fs.writeFile(typePath, this.type);
                    } else {
                        throw new Error(`Table type file not found`);
                    }
                }
            }

            //check table state
            let state = null;
            if (await utils.pathExists(statePath)) {
                state = await fs.readFile(statePath, 'utf8');
            }

            if (this.recreate) {
                await this._recreateTable();
                state = '1';
            }

            //load
            try {
                if (state === '1') {
                    await this._loadMeta();
                } else {
                    throw new Error('Table corrupted')
                }
            } catch(e) {
                if (this.autoRepair) {
                    console.error(e.message);
                    await this._recreateTable();
                } else {
                    throw e;
                }

                await this._loadMeta();
            }

            this.opened = true;
        } catch(e) {
            await this.close();
            throw new Error(`Open table (${query.tablePath}): ${e.message}`);
        } finally {
            this.opening = false;
        }
    }

    async close() {
        if (this.closed)
            return;

        this.opened = false;
        this.closed = true;

        await this._closeShards(true);
        await this.metaTable.close();
        await this.shardListTable.close();

        while (this.checkingTables) {
            await utils.sleep(10);
        }

        if (this.fileError) {
            try {
                await this._saveState('0');
            } catch(e) {
                //
            }
        }
    }

    _checkUniqueMeta(query) {
        if (query.hash) {
            for (const hash of utils.paramToArray(query.hash)) {
                if (hash.unique)
                    throw new Error(`Unique hashes are forbidden for this table type (${this.type})`);
            }
        }

        if (query.index) {
            for (const index of utils.paramToArray(query.index)) {
                if (index.unique)
                    throw new Error(`Unique indexes are forbidden for this table type (${this.type})`);
            }
        }
    }

    /*
    query = {
        quietIfExists: Boolean,
        flag:  Object || Array, {name: 'flag1', check: '(r) => r.id > 10'}
        hash:  Object || Array, {field: 'field1', type: 'string', depth: 11, allowUndef: false}
        index: Object || Array, {field: 'field1', type: 'string', depth: 11, allowUndef: false}
    }
    result = {}
    */
    async create(query) {
        this._checkErrors();

        this._checkUniqueMeta(query);

        for (const shard of this.shardList.keys()) {
            const table = await this._lockShard(shard);
            try {
                await table.create(query);
            } finally {
                await this._unlockShard(shard);
            }

            this.changedTables.push(table);
            this._checkTables(); //no await
        }

        const result = await this.metaTable.create(query);
        
        this.changedTables.push(this.metaTable);
        this._checkTables(); //no await

        return result;
    }

    /*
    query = {
        flag:  Object || Array, {name: 'flag1'}
        hash:  Object || Array, {field: 'field1'}
        index: Object || Array, {field: 'field1'}
    }
    result = {}
    */
    async drop(query) {
        this._checkErrors();

        for (const shard of this.shardList.keys()) {
            const table = await this._lockShard(shard);
            try {
                await table.drop(query);
            } finally {
                await this._unlockShard(shard);
            }

            this.changedTables.push(table);
            this._checkTables(); //no await
        }

        const result = await this.metaTable.drop(query);

        this.changedTables.push(this.metaTable);
        this._checkTables(); //no await

        return result;
    }

    /*
    result = {
        type: String,
        flag:  Array, [{name: 'flag1', check: '(r) => r.id > 10'}, ...]
        hash:  Array, [{field: 'field1', type: 'string', depth: 11, allowUndef: false}, ...]
        index: Array, [{field: 'field1', type: 'string', depth: 11, allowUndef: false}, ...]
        shardList: [{shard: 'string', num: 1, open: false, persistent: false, count: 10}, ...]
    }
    */
    async getMeta() {
        const result = await this.metaTable.getMeta();
        result.type = this.type;
        result.shardList = [];
        for (const shardRec of this.shardList.values()) {
            const lockRec = this.openedShardLockList.get(shardRec.id);

            result.shardList.push({
                shard: shardRec.id,
                num: shardRec.num,
                open: this.openedShardTables.has(shardRec.id),
                persistent: (lockRec ? lockRec.pers : 0),
                count: shardRec.count,
            });
        }
        result.count = this.infoShard.count;

        return result;
    }

    _getOpenedShardsFirst(shardsIter) {
        const shards = [];
        const tailShards = [];

        for (const shard of shardsIter) {
            if (this.openedShardTables.has(shard))
                shards.push(shard);
            else
                tailShards.push(shard);
        }

        return shards.concat(tailShards);
    }

    _parseQueryShards(query) {
        let selectedShards = new Set;
        if (!query.shards) {
            selectedShards = this.shardList.keys();
        } else {
            if (Array.isArray(query.shards)) {
                for (const shard of query.shards) {
                    if (!selectedShards.has(shard) && this.shardList.has(shard)) {
                        selectedShards.add(shard);
                    }
                }
            } else if (typeof(query.shards) === 'string') {
                const shardTestFunc = new Function(`'use strict'; return ${query.shards}`)();
                for (const shard of this.shardList.keys()) {
                    if (shardTestFunc(shard))
                        selectedShards.add(shard);
                }
            } else {
                throw new Error('Uknown query.shards param type');
            }
        }

        //opened shards first, to array
        selectedShards = this._getOpenedShardsFirst(selectedShards);
        return selectedShards;
    }

    /*
    query = {
        shards: ['shard1', 'shard2', ...] || '(s) => (s == 'shard1')',
        persistent: Boolean,//do not unload query.shards while persistent == true
        count: Boolean,
        where: `@@index('field1', 10, 20)`,
        distinct: 'fieldName' || Array,
        group: {byField: 'fieldName' || Array, byExpr: '(r) => groupingValue', countField: 'fieldName'},
        map: '(r) => ({id1: r.id, ...})',
        sort: '(a, b) => a.id - b.id',
        limit: 10,
        offset: 10,
    }
    result = Array
    */
    async select(query = {}) {
        this._checkErrors();

        //query.shards
        const selectedShards = this._parseQueryShards(query);
        const queryHasPersistent = utils.hasProp(query, 'persistent');

        const shardResult = [];
        if (query.count) {
            shardResult.push({count: this.infoShard.count});
        }

        //select from shards
        for (const shard of selectedShards) {
            const table = await this._lockShard(shard);
            try {
                if (queryHasPersistent) {
                    if (query.persistent)
                        this._updateOpenedShardLockList(shard, 0, 1);
                    else
                        this._updateOpenedShardLockList(shard, 0, -1);
                }

                const rows = await table.select(query);//select

                if (query.count) {
                    for (const row of rows)
                        row.shard = shard;
                }

                shardResult.push(rows);
            } finally {
                await this._unlockShard(shard);
            }
        }

        let result = [].concat(...shardResult);

        //sorting
        if (query.sort) {
            const sortFunc = new Function(`'use strict'; return ${query.sort}`)();
            result.sort(sortFunc);
        }

        //limits&offset
        if (utils.hasProp(query, 'limit') || utils.hasProp(query, 'offset')) {
            const offset = query.offset || 0;
            const limit = (utils.hasProp(query, 'limit') ? query.limit : result.length);
            result = result.slice(offset, offset + limit);
        }

        return result;
    }

    _genAutoShard() {
        const a = this.autoShard;
        while (a.list.length) {
            const last = a.list[a.list.length - 1];

            last.count--;
            if (last.count <= 0)
                a.list.pop();

            const shardRec = this.shardList.get(last.shard);
            
            if (!shardRec || shardRec.count < this.autoShardSize) {
                return last.shard;//return generated shard
            } else if (last.count > 0) {
                a.list.pop();
            }
        }

        //step 0 - check existing shards, opened shards to the end
        if (a.step === 0 && !a.list.length) {
            const opened = [];
            for (const [shard, shardRec] of this.shardList) {
                if (shardRec.count < this.autoShardSize) {

                    const listRec = {shard, count: this.autoShardSize - shardRec.count};

                    if (this.openedShardTables.has(shard)) {
                        opened.push(listRec);
                    } else {
                        a.list.push(listRec);
                    }
                }
            }

            a.list = a.list.concat(opened);

            a.step++;
        }

        //step 1 - generate new shard name, all existing are full
        if (a.step === 1 && !a.list.length) {
            const exists = new Set(this.shardList.keys());
            let i = 0;
            while (a.list.length < maxAutoShardListLength) {
                i++;
                const shard = `auto_${i}`;
                if (!exists.has(shard))
                    a.list.push({shard, count: this.autoShardSize});
            }

            a.list = a.list.reverse();
        }

        return this._genAutoShard();
    }

    /*
    query = {
        shardGen: '(r) => r.date',
    (!) rows: Array,
    }
    result = {
    (!) inserted: Number,
    (!) replaced: Number,//always 0
    (!) shardList: [{shard: 'name', inserted: Number}]
    }
    */
    async insert(query = {}) {
        this._checkErrors();

        try {
            if (!Array.isArray(query.rows)) {
                throw new Error('query.rows must be an array');
            }

            let shardGen = null;
            if (query.shardGen)
                shardGen = new Function(`'use strict'; return ${query.shardGen}`)();

            const rows = utils.cloneDeep(query.rows);
            //checks & shardedRows
            const shardedRows = new Map();
            for (const row of rows) {
                if (utils.hasProp(row, 'id'))
                    throw new Error(`row.id (${row.id}) use is not allowed for this table type (${this.type}) while insert`);

                if (!utils.hasProp(row, 'shard')) {
                    if (shardGen)
                        row.shard = shardGen(row);
                    else
                        throw new Error(`No row.shard field found for row: ${JSON.stringify(row)}`);
                }

                if (row.shard === '' || typeof(row.shard) !== 'string') 
                    throw new Error(`Wrong row.shard field value: '${row.shard}' for row: ${JSON.stringify(row)}`);

                //auto sharding
                if (row.shard === autoShardName)
                    row.shard = this._genAutoShard();

                let r = shardedRows.get(row.shard);
                if (!r) {
                    r = [];
                    shardedRows.set(row.shard, r);
                }
                r.push(row);
            }

            const result = {inserted: 0, replaced: 0, shardList: []};

            //opened shards first
            const shards = this._getOpenedShardsFirst(shardedRows.keys());

            //inserting
            for (const shard of shards) {
                const duiLock = this._getDUILock(shard);
                await duiLock.get();
                try {
                    let shardRowCount = 0;
                    const rows = shardedRows.get(shard);

                    const table = await this._lockShard(shard);
                    try {
                        const insResult = await table.insert({rows});//insert

                        this.changedTables.push(table);
                        result.inserted += insResult.inserted;
                        result.shardList.push({shard, inserted: insResult.inserted});
                        shardRowCount = table.rowsInterface.getAllIdsSize();
                    } finally {
                        await this._unlockShard(shard);
                    }

                    const shardRec = this.shardList.get(shard);
                    this.infoShard.count -= shardRec.count;
                    shardRec.count = shardRowCount;
                    this.infoShard.count += shardRowCount;
                    await this._saveShardRec(shardRec);
                    await this._saveShardRec(this.infoShard);
                } finally {
                    duiLock.ret();
                }
            }

            return result;
        } finally {
            this._checkTables(); //no await
        }
    }

    /*
    query = {
    (!) mod: '(r) => r.count++',
        shards: ['shard1', 'shard2', ...] || '(s) => (s == 'shard1')',
        where: `@@index('field1', 10, 20)`,
        sort: '(a, b) => a.id - b.id',
        limit: 10,
        offset: 10,
    }
    result = {
    (!) updated: Number,
    (!) shardList: [{shard: 'name', updated: Number}]
    }
    */
    async update(query = {}) {
        this._checkErrors();

        //query.shards
        const selectedShards = this._parseQueryShards(query);
        const result = {updated: 0, shardList: []};

        //update shards
        for (const shard of selectedShards) {
            const duiLock = this._getDUILock(shard);
            await duiLock.get();
            try {

                const table = await this._lockShard(shard);
                try {
                    const updResult = await table.update(query);//update

                    result.updated += updResult.updated;
                    result.shardList.push({shard, updated: updResult.updated});
                } finally {
                    await this._unlockShard(shard);
                }
            } finally {
                duiLock.ret();
            }
        }

        return result;
    }

    /*
    query = {
        shards: ['shard1', 'shard2', ...] || '(s) => (s == 'shard1')',
        where: `@@index('field1', 10, 20)`,
        sort: '(a, b) => a.id - b.id',
        limit: 10,
        offset: 10,
    }
    result = {
    (!) deleted: Number,
    (!) shardList: [{shard: 'name', deleted: Number}]
    }
    */
    async delete(query = {}) {
        this._checkErrors();

        //query.shards
        const selectedShards = this._parseQueryShards(query);

        const shardsToDelete = [];
        const result = {deleted: 0, shardList: []};
        //delete from shards
        for (const shard of selectedShards) {
            const duiLock = this._getDUILock(shard);
            await duiLock.get();
            try {
                let shardRowCount = 0;
                let delResult = null;

                const table = await this._lockShard(shard);
                try {
                    delResult = await table.delete(query);//delete

                    result.deleted += delResult.deleted;
                    result.shardList.push({shard, deleted: delResult.deleted});
                    shardRowCount = table.rowsInterface.getAllIdsSize();
                } finally {
                    await this._unlockShard(shard);
                }

                if (delResult.deleted > 0) {
                    const shardRec = this.shardList.get(shard);
                    this.infoShard.count -= shardRec.count;
                    shardRec.count = shardRowCount;
                    this.infoShard.count += shardRowCount;
                    await this._saveShardRec(shardRec);
                    await this._saveShardRec(this.infoShard);

                    if (shardRec.count <= 0)
                        shardsToDelete.push(shard);
                }
            } finally {
                duiLock.ret();
            }
        }

        if (result.deleted > 0)
            this.autoShard.step = 0;

        if (shardsToDelete.length)
            await this._delShards(shardsToDelete);

        return result;
    }        

    /*
    query = {
        message: String,
    }
    result = {}
    */
    async markCorrupted(query = {}) {
        this.fileError = query.message || 'Table corrupted';
        await this.close();

        return {};
    }

    /*
    query = {
    (!) toTablePath: String,
        filter: '(r) => true' || 'nodata',
        noMeta: Boolean,
    }
    result = {}
    */
    async clone(query = {}) {
        this._checkErrors();

        if (!query.toTablePath || typeof(query.toTablePath) !== 'string')
            throw new Error(`'query.toTablePath' parameter is required`);

        await this._cloneTable(this.tablePath, query.toTablePath, true, query.noMeta, query.filter);

        return {};
    }

    async _saveState(state) {
        await fs.writeFile(`${this.tablePath}/state`, state);
    }

    async _checkTables() {
        this.needCheckTables = true;
        if (this.checkingTables)
            return;

        try {
            this._checkErrors();
        } catch(e) {
            return;
        }

        this.checkingTables = true;
        try {
            await utils.sleep(0);

            while (this.needCheckTables) {
                this.needCheckTables = false;

                while (this.changedTables.length) {

                    const len = this.changedTables.length;
                    let i = 0;
                    while (i < len) {
                        const table = this.changedTables[i];
                        i++;

                        while (table.savingChanges) {
                            if (this.changedTables.indexOf(table, i) > 0)
                                break;
                            await utils.sleep(2);
                        }

                        if (table.fileError) {
                            this.fileError = table.fileError;
                            await this._saveState('0');
                            return;
                        }
                    }

                    this.changedTables = this.changedTables.slice(i);
                }
            }
        } catch(e) {
            console.error(e.message);
            this.fileError = e.message;
        } finally {
            this.checkingTables = false;
        }
    }

}

module.exports = ShardedTable;
