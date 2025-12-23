const responseCache = new Map();

function cacheGet(key, ttlMs) {
    const entry = responseCache.get(key);
    if (!entry) return null;
    if ((Date.now() - entry.ts) > ttlMs) {
        responseCache.delete(key);
        return null;
    }
    return entry.value;
}

function cacheSet(key, value) {
    responseCache.set(key, { ts: Date.now(), value });
}

module.exports = {
    cacheGet,
    cacheSet
};
