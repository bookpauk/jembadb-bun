const arrayClasses = {
    ArrayBuffer,
    Int8Array,
    Uint8Array,
    Uint8ClampedArray,

    Int16Array,
    Uint16Array,

    Int32Array,
    Uint32Array,

    Float32Array,
    Float64Array,
};

function replacer(key, value) {
    if (ArrayBuffer.isView(value) && arrayClasses[value.constructor.name]) {
        return {
            __constructor: value.constructor.name,
            __data: Buffer.from(value).toString('base64'),
        };
    }

    return value;
}

function reviver(key, value) {
    if (value.__constructor && arrayClasses[value.__constructor]) {
        const buf = Buffer.from(value.__data, 'base64');
        return new arrayClasses[value.__constructor](buf);
    }

    return value;
}

function encode(obj) {
    const str = JSON.stringify(obj);
    if (str.indexOf('{}') >= 0 || str.indexOf(':{"0":') >= 0)
        return JSON.stringify(obj, replacer);
    else
        return str;
}

function decode(str) {
    if (str.indexOf('__constructor') >= 0)
        return JSON.parse(str, reviver);
    else
        return JSON.parse(str);
}

module.exports = {
    encode,
    decode
};