// Simple block registry
const registry = new Map();

export function registerBlock(def) {
  if (!def || !def.id) throw new Error('Block definition requires id');
  registry.set(def.id, def);
}

export function getBlock(id) { return registry.get(id); }
export function listBlocks() { return Array.from(registry.keys()); }
