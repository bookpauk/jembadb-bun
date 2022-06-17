'use strict';
/*
    Total maximum rec count is unlimited.

    Limitations:
    - no rec.id while insert
    - rec.shard field required while insert    
    - no unique hashes and indexes
    - maximum rec count for one shard is ~16000000 (limitation of JS Map)
*/
const fs = require('fs').promises;
const utils = require('./utils');
const LockQueue = require('./LockQueue');

const BasicTable = require('./BasicTable');

const shardCountStep = 20*1000*1000;//must be greater than 16M
const maxFreeShardNumsLength = 100;

class ShardedTable {
    constructor() {
        this.type = 'sharded';

        this.autoIncrement = 0;
        this.fileError = '';

        this.lock = new LockQueue(100);

        this.metaTable = null; //basic table
        this.infoShard = {shard: '', count: 0};

        /*
        {
            shard: String,
            num: Number,
            count: Number,
        }
        */
        this.shardList = new Map();
        this.shardListTable = null;//basic table

        this.openedShardTables = new Map();//basic tables
        this.openedShardNames = new Set();
        this.freeShardNums = [];

        this.opened = false;
        this.closed = false;
        this.changedTables = [];

        //table open query
        this.openQuery = {};

        //table options defaults
        this.cacheShards = 1;
    }

    _checkErrors() {
        if (this.fileError)
            throw new Error(this.fileError);

        if (this.closed)
            throw new Error('Table closed');

        if (!this.opened)
            throw new Error('Table has not been opened yet');
    }

    async _recreateTable() {
        //TODO
    }

    async _loadMeta() {
        this.metaTable = new BasicTable();
        await this.metaTable.open({tablePath: `${this.tablePath}/meta`});

        this.shardListTable = new BasicTable();
        await this.shardListTable.open({tablePath: `${this.tablePath}/shards`});
        await this.shardListTable.create({
            quietIfExists: true,
            hash: {field: 'shard', depth: 100, unique: true},
        });

        const rows = await this.shardListTable.select({});//all
        for (const row of rows) {
            if (row.shard !== '') {                
                this.shardList.set(row.shard, row);
            } else {
                this.infoShard = row;
            }
        }
    }

    async _saveShardRec(shardRec) {
        await this.shardListTable.delete({where: `@@hash('shard', ${utils.esc(shardRec.shard)})`});
        await this.shardListTable.insert({rows: [shardRec]});
        this.changedTables.push(this.shardListTable);
        this._checkTables(); //no await
    }

    _shardTablePath(num) {
        if (num < 1000000)
            return `${this.tablePath}/s${num.toString().padStart(6, '0')}`;
        else
            return `${this.tablePath}/s${num.toString().padStart(12, '0')}`;
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

    async _closeShards(closeAll = false) {
        const maxSize = (closeAll ? 0 : this.cacheShards);
        while (this.openedShardTables.size > maxSize) {
            for (const [shard, table] of this.openedShardTables) {
                await table.close();
                this.openedShardTables.delete(shard);
                this.openedShardNames.delete(shard);
                break;
            }
        }
    }    

    async _openShard(shard) {
        if (this.openedShardNames.has(shard))
            return;

        let shardRec = this.shardList.get(shard);
        let isNew = !shardRec;
        if (isNew) {
            shardRec = {
                shard,
                num: this._getFreeShardNum(),
                count: 0,
            };

            await this._saveShardRec(shardRec);
            this.shardList.set(shardRec.shard, shardRec);
        }

        const newTable = new BasicTable();

        const query = utils.cloneDeep(this.openQuery);
        query.tablePath = this._shardTablePath(shardRec.num);
        await newTable.open(query);
        if (isNew)
            newTable.autoIncrement = shardCountStep*shardRec.num;

        this.openedShardTables.set(shardRec.shard, newTable);
        this.openedShardNames.add(shardRec.shard);

        await this._closeShards();
    }

    /*
    query: {
        tablePath: String,
        cacheSize: Number,
        cacheShards: Number, 1, for sharded table only
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
            this.cacheShards = query.cacheShards || 1;

            this.openQuery = query;            

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

        await this.lock.get();
        try {
            await this.metaTable.close();
            await this.shardListTable.close();
            await this._closeShards(true);

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
        } finally {
            this.lock.ret();
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
        this._checkUniqueMeta(query);
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
        shards: [{shard: 'string', num: 1, count: 10}, ...]
    }
    */
    async getMeta() {
        const result = await this.metaTable.getMeta();
        result.type = this.type;
        result.shards = Array.from(this.shardList.values()).map(
            (r) => ({shard: r.shard, num: r.num, count: r.count})
        );
        result.count = this.infoShard.count;

        return result;
    }

    /*
    query = {
        shards: ['shard1', 'shard2', ...] || '(s) => (s == 'shard1')',
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


    }

    /*
    query = {
        shardGen: '(r) => r.date',
    (!) rows: Array,
    }
    result = {
    (!) inserted: Number,
    (!) replaced: Number,//always 0
    }
    */
    async insert(query = {}) {
        this._checkErrors();

        await this.lock.get();
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

                let r = shardedRows.get(row.shard);
                if (!r) {
                    r = [];
                    shardedRows.set(row.shard, r);
                }
                r.push(row);
            }

            const result = {inserted: 0, replaced: 0};

            //first step - insert to already opened shard tables
            const shardedRowsSet = new Set(shardedRows.keys());
            const shortSet = (shardedRowsSet.size < this.openedShardNames.size ? shardedRowsSet : this.openedShardNames);
            for (const shard of shortSet) {
                if (shardedRowsSet.has(shard) && this.openedShardNames.has(shard)) {
                    //insert
                    const rows = shardedRows.get(shard);
                    const table = this.openedShardTables.get(shard);

                    const insResult = await table.insert({rows});
                    this.changedTables.push(table);
                    result.inserted += insResult.inserted;

                    const shardRec = this.shardList.get(shard);
                    this.infoShard.count -= shardRec.count;
                    shardRec.count = table.rowsInterface.getAllIdsSize();
                    this.infoShard.count += shardRec.count;
                    await this._saveShardRec(shardRec);
                    await this._saveShardRec(this.infoShard);

                    //update shardedRows
                    shardedRows.delete(shard);
                }
            }

            //second step - open & insert to other shard tables
            for (const [shard, rows] of shardedRows) {
                await this._openShard(shard);

                const table = this.openedShardTables.get(shard);

                const insResult = await table.insert({rows});
                this.changedTables.push(table);
                result.inserted += insResult.inserted;

                const shardRec = this.shardList.get(shard);
                this.infoShard.count -= shardRec.count;
                shardRec.count = table.rowsInterface.getAllIdsSize();
                this.infoShard.count += shardRec.count;
                await this._saveShardRec(shardRec);
                await this._saveShardRec(this.infoShard);
            }

            return result;
        } finally {
            this._checkTables(); //no await
            this.lock.ret();
        }
    }

    /*
    query = {
    (!) mod: '(r) => r.count++',
        where: `@@index('field1', 10, 20)`,
        sort: '(a, b) => a.id - b.id',
        limit: 10,
        offset: 10,
    }
    result = {
    (!) updated: Number,
    }
    */
    async update(query = {}) {
    }

    /*
    query = {
        where: `@@index('field1', 10, 20)`,
        sort: '(a, b) => a.id - b.id',
        limit: 10,
        offset: 10,
    }
    result = {
    (!) deleted: Number,
    }
    */
    async delete(query = {}) {
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