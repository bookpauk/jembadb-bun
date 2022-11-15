const fs = require('fs').promises;
const utils = require('./utils');

async function loadFile(filePath, allowCorrupted) {
    let buf = await fs.readFile(filePath);
    if (!buf.length)
        throw new Error(`TableRowsFile: file ${filePath} is empty`);

    const flag = buf[0];
    if (flag === 50) {//flag '2' ~ finalized && compressed
        const packed = Buffer.from(buf.buffer, buf.byteOffset + 1, buf.length - 1);
        const data = await utils.inflate(packed);
        buf = data.toString();
    } else if (flag === 49) {//flag '1' ~ finalized
        buf[0] = 32;//' '
        buf = buf.toString();
    } else {//flag '0' ~ not finalized
        buf[0] = 32;//' '
        const last = buf.length - 1;
        if (buf[last] === 44) {//','
            buf[last] = 93;//']'
            buf = buf.toString();
        } else {//corrupted or empty
            buf = buf.toString();
            if (allowCorrupted) {
                const lastComma = buf.lastIndexOf(',');
                if (lastComma >= 0)
                    buf = buf.substring(0, lastComma);
            }
            buf += ']';
        }
    }

    let result;
    try {
        result = JSON.parse(buf);
    } catch(e) {
        throw new Error(`load ${filePath} failed: ${e.message}`);
    }

    return result;
}

async function writeFinal(fileName, data, compressed) {
    data = JSON.stringify(data);

    if (!compressed) {
        await fs.writeFile(fileName, '1' + data);
    } else {
        let buf = Buffer.from(data);
        buf = await utils.deflate(buf, compressed);
        const fd = await fs.open(fileName, 'w');
        await fd.write('2');
        await fd.write(buf);
        await fd.close();
    }

    return Buffer.byteLength(data, 'utf8') + 1;
}


module.exports = {
    loadFile,
    writeFinal,
}