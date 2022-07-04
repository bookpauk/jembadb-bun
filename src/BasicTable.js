'use strict';
/*
    Limitations:
    - maximum rec count is ~16000000 (limitation of JS Map)
*/
const fs = require('fs').promises;
const utils = require('./utils');

const TableReducer = require('./TableReducer');
const TableRowsMem = require('./TableRowsMem');
const TableRowsFile = require('./TableRowsFile');
const LockQueue = require('./LockQueue');

const maxChangesLength = 10;

class BasicTable {
    constructor() {
        this.type = 'basic';

        this.rowsInterface = null;

        this.autoIncrement = 0;
        this.fileError = '';

        this.lock = new LockQueue(100);

        this.opened = false;
        this.closed = false;
        this.deltaStep = 0;
        this.changes = [];

        //table options defaults
        this.inMemory = false;
        this.cacheSize = 5;
        this.compressed = 0;
        this.recreate = false;
        this.autoRepair = false;
        this.forceFileClosing = false;
    }

    _checkErrors() {
        if (this.fileError)
            throw new Error(this.fileError);

        if (this.closed)
            throw new Error('Table closed');

        if (!this.opened)
            throw new Error('Table has not been opened yet');
    }

    async _waitForSaveChanges() {
        if (this.changes.length > maxChangesLength) {
            let i = this.changes.length - maxChangesLength;
            while (i > 0 && this.changes.length > maxChangesLength) {
                i--;
                await utils.sleep(10);
            }
        }
    }

    async _cloneTable(srcTablePath, destTablePath, cloneSelf = false, noMeta = false, filter) {
        if (this.inMemory)
            throw new Error('_cloneTable error: inMemory source table');

        await fs.rmdir(destTablePath, { recursive: true });
        await fs.mkdir(destTablePath, { recursive: true });

        // '(r) => true' || 'nodata',
        let filterFunc = null;
        const nodata = (filter === 'nodata');
        if (filter && !nodata) {
            filterFunc = new Function(`'use strict'; return ${filter}`)();
        } else {
            filterFunc = () => true;
        }

        let tableRowsFileSrc;
        if (cloneSelf) {
            srcTablePath = this.tablePath;
            tableRowsFileSrc = this.tableRowsFile;
        } else {
            tableRowsFileSrc = new TableRowsFile(srcTablePath, this.cacheSize);
        }

        const tableRowsFileDest = new TableRowsFile(destTablePath, this.cacheSize, this.compressed);
        const reducerDest = new TableReducer(false, destTablePath, this.compressed, tableRowsFileDest);

        try {
            if (!nodata)
                await tableRowsFileSrc.loadCorrupted();
        } catch (e) {
            console.error(e);
        }

        try {
            if (!noMeta)
                await reducerDest._load(true, `${srcTablePath}/meta.0`);
        } catch (e) {
            console.error(e);
        }

        const putRows = async(rows) => {
            const oldRows = [];
            const newRows = [];
            const newRowsStr = [];
            //checks
            for (const row of rows) {                
                if (!row) {
                    continue;
                }

                const t = typeof(row.id);
                if  (t !== 'number' && t !== 'string') {
                    continue;
                }

                const oldRow = await tableRowsFileDest.getRow(row.id);

                if (oldRow) {
                    continue;
                }

                let str = '';
                try {
                    str = JSON.stringify(row);//because of stringify errors
                } catch(e) {
                    continue;
                }

                newRows.push(row);
                oldRows.push({});
                newRowsStr.push(str);
            }

            try {
                //reducer
                reducerDest._update(oldRows, newRows, 1);

                //insert
                for (let i = 0; i < newRows.length; i++) {
                    const newRow = newRows[i];
                    const newRowStr = newRowsStr[i];

                    tableRowsFileDest.setRow(newRow.id, newRow, newRowStr, 1);
                }

                await tableRowsFileDest.saveDelta(1);
                await reducerDest._saveDelta(1);
            } catch(e) {
                console.error(e);
            }
        };

        if (!nodata) {
            let rows = [];
            for (const id of tableRowsFileSrc.getAllIds()) {
                if (this.closed)
                    throw new Error('Table closed');

                let row = null;
                try {
                    row = await tableRowsFileSrc.getRow(id);
                } catch(e) {
                    console.error(e);
                    continue;
                }

                if (!filterFunc(row))
                    continue;

                rows.push(row);
                if (rows.length > 1000) {
                    await putRows(rows);
                    rows = [];
                }
            }
            if (rows.length)
                await putRows(rows);
        }

        await tableRowsFileDest.saveDelta(0);

        const delta = reducerDest._getDelta(0);
        delta.dumpMeta = true;
        await reducerDest._saveDelta(0);

        await tableRowsFileSrc.destroy();
        await reducerDest._destroy();
        await tableRowsFileDest.destroy();        

        await fs.writeFile(`${destTablePath}/state`, '1');
        await fs.writeFile(`${destTablePath}/type`, this.type);
    }

    async _recreateTable() {
        const tempTablePath = `${this.tablePath}___temporary_recreating`;

        await this._cloneTable(this.tablePath, tempTablePath);

        await fs.rmdir(this.tablePath, { recursive: true });
        await fs.rename(tempTablePath, this.tablePath);
    }

    /*
    query: {
        tablePath: String,
        cacheSize: Number,
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

            if (this.inMemory) {
                this.rowsInterface = new TableRowsMem();
                this.reducer = new TableReducer(this.inMemory, '', 0, this.rowsInterface);
            } else {
                if (!query.tablePath)
                    throw new Error(`'query.tablePath' parameter is required`);

                this.tablePath = query.tablePath;
                this.cacheSize = query.cacheSize || 5;
                this.compressed = query.compressed || 0;
                this.recreate = query.recreate || false;
                this.autoRepair = query.autoRepair || false;
                this.forceFileClosing = query.forceFileClosing || false;

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

                //table rows & reducer
                this.tableRowsFile = new TableRowsFile(query.tablePath, this.cacheSize, this.compressed);
                this.rowsInterface = this.tableRowsFile;

                this.reducer = new TableReducer(this.inMemory, this.tablePath, this.compressed, this.rowsInterface);

                //load
                try {
                    if (state === '1') {
                        // load tableRowsFile & reducer
                        this.autoIncrement = await this.tableRowsFile.load();
                        await this.reducer._load();
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
                    // load tableRowsFile & reducer
                    this.autoIncrement = await this.tableRowsFile.load();
                    await this.reducer._load();
                }
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
        if (this.closing || this.closed)
            return;

        this.closing = true;

        this.lock.queueSize++;//if 'lock queue is too long' error
        await this.lock.get();
        try {
            this.opened = false;
            this.closed = true;

            if (!this.inMemory) {
                while (this.savingChanges) {
                    await utils.sleep(10);
                }
            }

            if (this.fileError) {
                try {
                    await this._saveState('0');
                } catch(e) {
                    //
                }
            }

            //for GC
            if (this.reducer)
                await this.reducer._destroy();
            this.reducer = null;

            if (this.rowsInterface)
                await this.rowsInterface.destroy();
            this.rowsInterface = null;
            this.tableRowsFile = null;
        } finally {
            this.lock.ret();
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

        await this.lock.get();
        try {
            this.deltaStep++;
            try {
                if (query.flag) {
                    for (const flag of utils.paramToArray(query.flag)) {
                        await this.reducer._addFlag(flag, query.quietIfExists, this.deltaStep);
                    }
                }

                if (query.hash) {
                    for (const hash of utils.paramToArray(query.hash)) {
                        await this.reducer._addHash(hash, query.quietIfExists, this.deltaStep);
                    }
                }

                if (query.index) {
                    for (const index of utils.paramToArray(query.index)) {
                        await this.reducer._addIndex(index, query.quietIfExists, this.deltaStep);
                    }
                }

                this.changes.push([this.deltaStep, 1]);
            } catch(e) {
                this.changes.push([this.deltaStep, 0]);
                throw e;
            }

            return {};
        } finally {
            this._saveChanges();//no await
            this.lock.ret();
        }
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

        await this.lock.get();
        try {
            this.deltaStep++;
            try {
                if (query.flag) {
                    for (const flag of utils.paramToArray(query.flag)) {
                        await this.reducer._delFlag(flag.name, this.deltaStep);
                    }
                }

                if (query.hash) {
                    for (const hash of utils.paramToArray(query.hash)) {
                        await this.reducer._delHash(hash.field, this.deltaStep);
                    }
                }

                if (query.index) {
                    for (const index of utils.paramToArray(query.index)) {
                        await this.reducer._delIndex(index.field, this.deltaStep);
                    }
                }

                this.changes.push([this.deltaStep, 1]);
            } catch(e) {
                this.changes.push([this.deltaStep, 0]);
                throw e;
            }

            return {};
        } finally {
            this._saveChanges();//no await
            this.lock.ret();
        }
    }

    /*
    result = {
        type: String,
        count: Number,
        flag:  Array, [{name: 'flag1', check: '(r) => r.id > 10'}, ...]
        hash:  Array, [{field: 'field1', type: 'string', depth: 11, allowUndef: false}, ...]
        index: Array, [{field: 'field1', type: 'string', depth: 11, allowUndef: false}, ...]
    }
    */
    async getMeta() {
        this._checkErrors();

        return {
            type: this.type,
            count: this.rowsInterface.getAllIdsSize(),
            flag: this.reducer._listFlag(),
            hash: this.reducer._listHash(),
            index: this.reducer._listIndex(),
        };
    }

    _prepareWhere(where) {
        if (typeof(where) !== 'string')
            throw new Error('query.where must be a string');

        return `async(__tr) => {${where.replace(/@@/g, 'return await __tr.').replace(/@/g, 'await __tr.')}}`;
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
        this._checkErrors();

        let ids;//iterator
        //where condition
        if (query.where) {
            const where = this._prepareWhere(query.where);
            const whereFunc = new Function(`'use strict'; return ${where}`)();

            ids = await whereFunc(this.reducer);
        } else {
            ids = this.rowsInterface.getAllIds();
        }

        //grouping
        let inGroup = () => false;
        const groupMap = new Map();
        const doCount = query.group && query.group.countField;

        if (query.distinct || query.group) {
            if (query.distinct && query.group)
                throw new Error(`One of query.distinct or query.qroup params expected, but not both`);

            let groupByField = null;
            let groupByExpr = null;
            if (query.distinct)
                groupByField = (Array.isArray(query.distinct) ? query.distinct : [query.distinct]);
            
            if (query.group) {
                if (query.group.byField && query.group.byExpr)
                    throw new Error(`One of query.qroup.byField or query.qroup.byExpr params expected, but not both`);
                if (query.group.byField) {
                    groupByField = (Array.isArray(query.group.byField) ? query.group.byField : [query.group.byField]);
                } else if (query.group.byExpr) {
                    groupByExpr = new Function(`'use strict'; return ${query.group.byExpr}`)();
                }
            }

            //returns (count - 1) group size
            inGroup = (row) => {
                let groupingValue = '';
                if (groupByField) {
                    for (const field of groupByField) {
                        const value = JSON.stringify(row[field]);
                        groupingValue += value.length + value;
                    }
                } else if (groupByExpr) {
                    groupingValue = groupByExpr(row);
                }

                if (groupMap.has(groupingValue)) {
                    if (doCount) {
                        const count = groupMap.get(groupingValue);
                        groupMap.set(groupingValue, count + 1);
                        return count;
                    }
                    return 1;
                }

                groupMap.set(groupingValue, 1);
                return 0;
            };
        }

        //selection
        let found = [];
        if (query.count && !query.distinct && !query.group) {//optimization
            if (query.where) {
                let count = 0;
                for (const id of ids) {
                    if (this.rowsInterface.hasRow(id))
                        count++;
                }
                found = [{count}];
            } else {
                found = [{count: this.rowsInterface.getAllIdsSize()}];
            }
        } else {//full running
            for (const id of ids) {
                const row = await this.rowsInterface.getRow(id);

                if (row && !inGroup(row)) {
                    found.push(row);
                }
            }

            if (query.count) {
                found = [{count: found.length}];
            }
        }

        found = utils.cloneDeep(found);//for safu

        //grouping count field
        if (doCount) {
            for (const row of found) {
                row[query.group.countField] = inGroup(row);
            }
        }

        //mapping
        let result = [];
        if (query.map) {
            const mapFunc = new Function(`'use strict'; return ${query.map}`)();

            for (const row of found) {
                result.push(mapFunc(row));
            }
        } else {
            result = found;
        }

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

    /*
    query = {
        ignore: Boolean,
        replace: Boolean,
    (!) rows: Array,
    }
    result = {
    (!) inserted: Number,
    (!) replaced: Number,
    (!) lastInsertId: Number,
    }
    */
    async insert(query = {}) {
        this._checkErrors();

        await this.lock.get();
        try {
            if (query.ignore && query.replace)
                throw new Error(`One of query.ignore or query.replace params expected, but not both`);

            if (!Array.isArray(query.rows)) {
                throw new Error('query.rows must be an array');
            }

            const newRowsSrc = utils.cloneDeep(query.rows);
            const replace = query.replace;
            const ignore = query.ignore;

            //autoIncrement correction
            for (const newRow of newRowsSrc) {
                if (typeof(newRow.id) === 'number' && newRow.id >= this.autoIncrement)
                    this.autoIncrement = newRow.id + 1;
            }

            const newRows = [];
            const oldRows = [];
            const newRowsStr = [];
            //checks
            for (const newRow of newRowsSrc) {
                if (newRow.id === undefined) {
                    newRow.id = this.autoIncrement;
                    this.autoIncrement++;
                }

                const t = typeof(newRow.id);
                if  (t !== 'number' && t !== 'string') {
                    throw new Error(`Row id bad type, 'number' or 'string' expected, got ${t}`);
                }

                const oldRow = await this.rowsInterface.getRow(newRow.id);

                if (!replace && oldRow) {
                    if (ignore) {
                        continue;
                    } else {
                        throw new Error(`Record id:${newRow.id} already exists`);
                    }
                }

                newRows.push(newRow);
                oldRows.push((oldRow ? oldRow : {}));
                newRowsStr.push(JSON.stringify(newRow));//because of stringify errors
            }

            const result = {inserted: 0, replaced: 0, lastInsertId: -1};
            this.deltaStep++;
            try {
                //reducer
                this.reducer._update(oldRows, newRows, this.deltaStep);

                //insert
                for (let i = 0; i < newRows.length; i++) {
                    const newRow = newRows[i];
                    const newRowStr = newRowsStr[i];
                    const oldRow = oldRows[i];

                    this.rowsInterface.setRow(newRow.id, newRow, newRowStr, this.deltaStep);

                    result.lastInsertId = newRow.id;

                    if (oldRow.id !== undefined)
                        result.replaced++;
                    else
                        result.inserted++;
                }

                this.changes.push([this.deltaStep, 1]);
            } catch(e) {
                this.changes.push([this.deltaStep, 0]);
                throw e;
            }

            await this._waitForSaveChanges();
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
        this._checkErrors();

        await this.lock.get();
        try {
            if (typeof(query.mod) !== 'string') {
                throw new Error('query.mod must be a string');
            }
            const modFunc = new Function(`'use strict'; return ${query.mod}`)();

            //where
            let ids;//iterator
            if (query.where) {
                const where = this._prepareWhere(query.where);
                const whereFunc = new Function(`'use strict'; return ${where}`)();

                ids = await whereFunc(this.reducer);
            } else {
                ids = this.rowsInterface.getAllIds();
            }

            //oldRows
            let oldRows = [];
            for (const id of ids) {
                const oldRow = await this.rowsInterface.getRow(id);

                if (oldRow) {
                    oldRows.push(oldRow);
                }
            }

            if (query.sort) {
                const sortFunc = new Function(`'use strict'; return ${query.sort}`)();
                oldRows.sort(sortFunc);
            }
            let newRows = utils.cloneDeep(oldRows);

            if (utils.hasProp(query, 'limit') || utils.hasProp(query, 'offset')) {
                const offset = query.offset || 0;
                const limit = (utils.hasProp(query, 'limit') ? query.limit : newRows.length);
                newRows = newRows.slice(offset, offset + limit);
                oldRows = oldRows.slice(offset, offset + limit);
            }

            //mod & checks
            const context = {};
            const newRowsStr = [];
            for (const newRow of newRows) {
                modFunc(newRow, context);

                const t = typeof(newRow.id);
                if  (t !== 'number' && t !== 'string') {
                    throw new Error(`Row id bad type, 'number' or 'string' expected, got ${t}`);
                }

                //autoIncrement correction
                if (t === 'number' && newRow.id >= this.autoIncrement)
                    this.autoIncrement = newRow.id + 1;

                newRowsStr.push(JSON.stringify(newRow));//because of stringify errors
            }

            this.deltaStep++;
            const result = {updated: 0};
            try {
                //reducer
                this.reducer._update(oldRows, newRows, this.deltaStep);

                //replace
                for (let i = 0; i < newRows.length; i++) {
                    const newRow = newRows[i];
                    const newRowStr = newRowsStr[i];

                    // oldRow.id === newRow.id always here, so
                    this.rowsInterface.setRow(newRow.id, newRow, newRowStr, this.deltaStep);

                    result.updated++;
                }

                this.changes.push([this.deltaStep, 1]);
            } catch(e) {
                this.changes.push([this.deltaStep, 0]);
                throw e;
            }

            await this._waitForSaveChanges();
            return result;
        } finally {
            this._saveChanges();//no await
            this.lock.ret();
        }
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
        this._checkErrors();

        await this.lock.get();
        try {
            //where
            let ids;//iterator
            if (query.where) {
                const where = this._prepareWhere(query.where);
                const whereFunc = new Function(`'use strict'; return ${where}`)();

                ids = await whereFunc(this.reducer);
            } else {
                ids = this.rowsInterface.getAllIds();
            }

            //oldRows
            let oldRows = [];
            let newRows = [];
            for (const id of ids) {
                const oldRow = await this.rowsInterface.getRow(id);

                if (oldRow) {
                    oldRows.push(oldRow);
                    newRows.push({});
                }
            }

            if (query.sort) {
                const sortFunc = new Function(`'use strict'; return ${query.sort}`)();
                oldRows.sort(sortFunc);
            }

            if (utils.hasProp(query, 'limit') || utils.hasProp(query, 'offset')) {
                const offset = query.offset || 0;
                const limit = (utils.hasProp(query, 'limit') ? query.limit : newRows.length);
                newRows = newRows.slice(offset, offset + limit);
                oldRows = oldRows.slice(offset, offset + limit);
            }

            this.deltaStep++;
            const result = {deleted: 0};
            try {
                //reducer
                this.reducer._update(oldRows, newRows, this.deltaStep);

                //delete
                for (let i = 0; i < oldRows.length; i++) {
                    const oldRow = oldRows[i];
                        
                    this.rowsInterface.deleteRow(oldRow.id, this.deltaStep);

                    result.deleted++;
                }

                this.changes.push([this.deltaStep, 1]);
            } catch(e) {
                this.changes.push([this.deltaStep, 0]);
                throw e;
            }

            await this._waitForSaveChanges();
            return result;
        } finally {
            this._saveChanges();//no await
            this.lock.ret();
        }
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

        if (this.inMemory) {
            throw new Error(`inMemory table can't be cloned`);
        }

        if (!query.toTablePath || typeof(query.toTablePath) !== 'string')
            throw new Error(`'query.toTablePath' parameter is required`);

        await this.lock.get();
        try {
            while (this.savingChanges) {
                await utils.sleep(1);
            }

            await this._cloneTable(this.tablePath, query.toTablePath, true, query.noMeta, query.filter);

            return {};
        } finally {
            this.lock.ret();
        }
    }

    async _saveState(state) {
        await fs.writeFile(`${this.tablePath}/state`, state);
    }

    async _saveChanges() {
        this.needSaveChanges = true;
        if (this.savingChanges)
            return;

        if (this.inMemory) {
            this.changes = [];
            return;
        }

        try {
            this._checkErrors();
        } catch(e) {
            return;
        }

        this.savingChanges = true;
        try {            
            await utils.sleep(0);

            while (this.needSaveChanges) {
                this.needSaveChanges = false;

                await this._saveState('0');
                while (this.changes.length) {

                    const len = this.changes.length;
                    let i = 0;
                    while (i < len) {
                        const [deltaStep, isOk] = this.changes[i];
                        i++;

                        if (isOk) {
                            await this.tableRowsFile.saveDelta(deltaStep);
                            await this.reducer._saveDelta(deltaStep);
                        } else {
                            await this.tableRowsFile.cancelDelta(deltaStep);
                            await this.reducer._cancelDelta(deltaStep);
                        }
                    }

                    this.changes = this.changes.slice(i);
                }
                await this._saveState('1');

                if (this.forceFileClosing) {
                    await this.tableRowsFile.closeAllFiles();
                    await this.reducer._closeAllFiles();
                }
            }
        } catch(e) {
            console.error(e.message);
            this.fileError = e.message;
        } finally {
            this.savingChanges = false;
        }
    }

}

module.exports = BasicTable;