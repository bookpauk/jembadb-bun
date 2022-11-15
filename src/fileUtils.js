const fs = require('fs').promises;
const v8 = require('node:v8');

const utils = require('./utils');

async function openFile(fileName) {
    const exists = await utils.pathExists(fileName);

    const fd = await fs.open(fileName, 'a');
    if (!exists) {
        const flag = new Uint8Array([0]);
        await fd.write(flag);
    }

    return fd;
}

async function loadJsonFile(buf, allowCorrupted) {
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

    return JSON.parse(buf);
}

async function loadFile(fileName, allowCorrupted) {
    let buf = await fs.readFile(fileName);
    if (!buf.length)
        throw new Error(`loadFile: file ${fileName} is empty`);

    try {
        if (buf[0] > 10)//old json format
            return await loadJsonFile(buf, allowCorrupted);

        //binary format
        let result;
        const flag = buf[0];
        const data = new DataView(buf.buffer, buf.byteOffset + 1);
        if (flag === 2) {//flag '2' ~ finalized && compressed
            buf = await utils.inflate(data);
            result = v8.deserialize(buf);
        } else if (flag === 1) {//flag '1' ~ finalized
            result = v8.deserialize(data);
        } else if (flag === 0) {//flag '0' ~ not finalized
            //array of records
            result = [];

            try {
                let offset = 0;
                while (offset < data.byteLength) {
                    const recLen = data.getUint32(offset);
                    const dv = new DataView(buf.buffer, buf.byteOffset + offset + 5, recLen);
                    result.push(v8.deserialize(dv));
                    offset += recLen + 4;
                }
            } catch (e) {
                if (!allowCorrupted)
                    throw e;
            }
        } else {
            throw new Error(`unknown file format, header flag: ${flag}`);
        }

        return result;
    } catch(e) {
        throw new Error(`load ${fileName} failed: ${e.message}`);
    }
}

async function writeFinal(fileName, data, compressed) {
    data = v8.serialize(data);
    const flag = new Uint8Array(1);
    let buf;
    if (!compressed) {
        flag[0] = 1;
        buf = data;
    } else {
        flag[0] = 2;
        buf = await utils.deflate(data, compressed);
    }

    const fd = await fs.open(fileName, 'w');
    await fd.write(flag);
    await fd.write(buf);
    await fd.close();

    return buf.length + 1;
}

async function appendRecs(fd, recs) {
    let dataLen = 0;
    for (const rec of recs) {
        dataLen += rec.length + 4;
    }

    const buf = new Uint8Array(dataLen);
    const dv = new DataView(buf.buffer, buf.byteOffset);

    let offset = 0;
    for (const rec of recs) {
        dv.setUint32(offset, rec.length);
        buf.set(rec, offset + 4);
        offset += rec.length + 4;
    }

    await fd.write(buf);
}

module.exports = {
    openFile,
    loadFile,
    writeFinal,
    appendRecs,
}