const Table = require('./Table');

class TableMem extends Table {
    constructor() {
        super();

        this.type = 'memory';
        this.inMemory = true;
    }

    /*
    query = {
    (!) toTableInstance: TableMem,
        filter: '(r) => true' || 'nodata',
        cloneMeta: Boolean,
    }
    result = {}
    */
    async clone(query = {}) {
        if (!query.toTableInstance)
            throw new Error(`'query.toTableInstance' parameter is required`);

        await this.openingLock.wait();
        this._checkErrors();

        await this.lock.get();
        try {
            const newTableInstance = query.toTableInstance;

            if (query.cloneMeta) {
                const meta = await this.getMeta();
                await newTableInstance.create(meta);
            }

            let filterFunc = null;
            let nodata = (query.filter === 'nodata');
            if (query.filter && !nodata) {
                filterFunc = new Function(`'use strict'; return ${query.filter}`)();
            } else {
                filterFunc = () => true;
            }

            if (!nodata) {
                const rows = await this.select();
                const newRows = rows.filter(filterFunc);
                await newTableInstance.insert({rows: newRows});
            }

            return {};
        } finally {
            this.lock.ret();
        }
    }


}

module.exports = TableMem;