'use strict';

class TableFlag {
    constructor(checkCode) {
        this.checkCode = checkCode;
        this.checkFunc = new Function(`'use strict'; return ${checkCode}`)();

        this.flag = new Set();
    }

    add(row) {
        if (this.checkFunc(row)) {
            this.flag.add(row.id);
            return true;
        }
        return false;
    }

    del(row) {
        this.flag.delete(row.id);
    }
}

module.exports = TableFlag;