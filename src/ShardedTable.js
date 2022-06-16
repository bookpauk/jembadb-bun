'use strict';
/*
    Maximum rec count is unlimited.

    Limitations:
    - no rec.id while insert
    - rec.shard field required while insert    
    - no unique hashes and indexes
    - maximum shard rec count is ~16000000 (limitation of JS Map)
*/
const fs = require('fs').promises;
const utils = require('./utils');
const LockQueue = require('./LockQueue');

const BasicTable = require('./BasicTable');

class ShardedTable {
    constructor() {
        this.type = 'sharded';

        this.autoIncrement = 0;
        this.fileError = '';

        this.lock = new LockQueue(100);

        this.meta = null; //basic table
        this.shards = null;//basic table

        this.metaShard = {shard: '', count: 0};
        this.shardList = new Map();
        this.openedShards = new Map();//basic tables

        this.opened = false;
        this.closed = false;

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
        this.meta = new BasicTable();
        await this.meta.open({tablePath: `${this.tablePath}/meta`});

        this.shards = new BasicTable();
        await this.shards.open({tablePath: `${this.tablePath}/shards`});
        await this.shards.create({
            quietIfExists: true,
            hash: {field: 'shard', depth: 100, unique: true},
        });

        const rows = await this.shards.select({});//all
        for (const row of rows) {
            if (row.shard !== '') {                
                this.shardList.set(row.shard, row);
            } else {
                this.metaShard = row;
            }
        }
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
        this._checkUniqueMeta(query);
        return await this.meta.create(query);
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
        return await this.meta.drop(query);
    }

    /*
    result = {
        type: String,
        flag:  Array, [{name: 'flag1', check: '(r) => r.id > 10'}, ...]
        hash:  Array, [{field: 'field1', type: 'string', depth: 11, allowUndef: false}, ...]
        index: Array, [{field: 'field1', type: 'string', depth: 11, allowUndef: false}, ...]
    }
    */
    async getMeta() {
        return await this.meta.getMeta();
    }

    /*
    query = {
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
    }

    /*
    query = {
        replace: Boolean,
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

            for (const row of query.rows) {
                if (utils.hasProp(row, 'id'))
                    throw new Error(`row.id (${row.id}) use is not allowed for this table type (${this.type}) while insert`);
                if (!utils.hasProp(row, 'shard'))
                    throw new Error(`No row.shard field found for row.id: ${row.id}`);
                if (row.shard === '' || typeof(row.shard) !== 'string') 
                    throw new Error(`Wrong row.shard field value: '${row.shard}' for row.id: ${row.id}`);
            }

            const result = {inserted: 0, replaced: 0};

            return result;
        } finally {
            this._saveChanges();//no await
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

}

module.exports = ShardedTable;