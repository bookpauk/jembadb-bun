function encode(obj) {
    return JSON.stringify(obj);
}

function decode(str) {
    return JSON.parse(str);
}

module.exports = {
    encode,
    decode
};