const STORAGE_KEY = 'optionsHub.serverBase';
const PORT_KEY = 'optionsHub.apiPort';
let cachedBase = null;
let detectionPromise = null;

function safeGet(storage, key) {
  try {
    return storage?.getItem(key) || null;
  } catch (_) {
    return null;
  }
}

function safeSet(storage, key, value) {
  try {
    storage?.setItem(key, value);
  } catch (_) {
    /* ignore */
  }
}

function safeRemove(storage, key) {
  try {
    storage?.removeItem(key);
  } catch (_) {
    /* ignore */
  }
}

function normalizeBase(input) {
  if (!input) return null;
  try {
    const url = new URL(input, window.location.origin);
    return url.origin.replace(/\/$/, '');
  } catch (_) {
    return null;
  }
}

function unique(list) {
  return Array.from(new Set(list.filter(Boolean)));
}

function buildPortCandidates() {
  const ports = new Set();
  const locPort = window.location.port;
  if (locPort) ports.add(locPort);
  const storedPort = safeGet(window.localStorage, PORT_KEY);
  if (storedPort) ports.add(storedPort);
  const query = new URLSearchParams(window.location.search);
  const queryPort = query.get('apiPort') || query.get('api-port');
  if (queryPort) ports.add(queryPort);
  ports.add('9000');
  ports.add('9001');
  ports.add('9004');
  for (let p = 8995; p <= 9010; p++) {
    ports.add(String(p));
  }
  ports.add('');
  return Array.from(ports);
}

function buildHostCandidates() {
  const hosts = new Set();
  if (window.location.hostname) hosts.add(window.location.hostname);
  hosts.add('localhost');
  hosts.add('127.0.0.1');
  return Array.from(hosts);
}

function buildCandidateBases() {
  const explicit = [
    normalizeBase(window.SERVER_BASE),
    normalizeBase(window.SERVER_ORIGIN),
    normalizeBase(safeGet(window.localStorage, STORAGE_KEY)),
    normalizeBase(safeGet(window.sessionStorage, STORAGE_KEY))
  ];
  const fromQuery = new URLSearchParams(window.location.search).get('apiBase');
  if (fromQuery) explicit.push(normalizeBase(fromQuery));

  const protocol = window.location.protocol || 'http:';
  const hosts = buildHostCandidates();
  const ports = buildPortCandidates();
  const combos = hosts.flatMap(host => ports.map(port => {
    if (!port) return `${protocol}//${host}`;
    return `${protocol}//${host}:${port}`;
  }));

  return unique([...explicit, ...combos]);
}

async function pingBase(base) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch(`${base}/api/test-gex`, { signal: controller.signal, credentials: 'omit' });
    clearTimeout(timeout);
    if (res.ok) {
      return base;
    }
  } catch (_) {
    clearTimeout(timeout);
  }
  return null;
}

async function detectServerBase() {
  const candidates = buildCandidateBases();
  for (const base of candidates) {
    const result = await pingBase(base);
    if (result) {
      cachedBase = result;
      safeSet(window.localStorage, STORAGE_KEY, result);
      return result;
    }
  }
  cachedBase = normalizeBase(window.location.origin) || 'http://localhost:9000';
  return cachedBase;
}

export async function getServerBase() {
  if (cachedBase) {
    const ok = await pingBase(cachedBase);
    if (ok) return cachedBase;
    cachedBase = null;
    safeRemove(window.localStorage, STORAGE_KEY);
  }
  if (!detectionPromise) {
    detectionPromise = detectServerBase().finally(() => {
      detectionPromise = null;
    });
  }
  return detectionPromise;
}
