const fs = require('fs').promises;

const utils = require('./utils');
const mson = require('./mson');

async function openFile(fileName) {
    const exists = await utils.pathExists(fileName);

    const fd = await fs.open(fileName, 'a');
    if (!exists) {
        await fd.write('0[');
    }

    return fd;
}

async function loadFile(fileName, allowCorrupted) {
    try {
        let buf = await fs.readFile(fileName);
        if (!buf.length)
            throw new Error(`loadFile: file ${fileName} is empty`);

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

        return mson.decode(buf);
    } catch(e) {
        throw new Error(`loadFile: parsing ${fileName} failed: ${e.message}`);
    }
}

async function writeFinal(fileName, data, compressed) {
    data = mson.encode(data);

    let flag;
    let buf;
    if (!compressed) {
        flag = '1';
        buf = data;
    } else {
        flag = '2';
        buf = await utils.deflate(data, compressed);
    }

    const fd = await fs.open(fileName, 'w');
    await fd.write(flag);
    await fd.write(buf);
    await fd.close();

    return buf.byteLength + 1;
}

async function appendRecs(fd, recs) {
    if (!recs.length)
        return;

    await fd.write(recs.join(',') + ',');
}

module.exports = {
    openFile,
    loadFile,
    writeFinal,
    appendRecs,
}