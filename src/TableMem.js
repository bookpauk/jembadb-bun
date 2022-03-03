const Table = require('./Table');

class TableMem extends Table {
    constructor() {
        super();

        this.type = 'memory';
        this.inMemory = true;
    }
}

module.exports = TableMem;