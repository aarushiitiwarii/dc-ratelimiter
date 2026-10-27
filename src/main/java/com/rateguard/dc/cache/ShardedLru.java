package com.rateguard.dc.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Bounded, sharded LRU cache (the edge node's L1 cache). Shards keep lock
 * contention low; evicted entries are handed to a callback so reserved tokens
 * can be returned to the coordinator instead of being lost.
 */
public final class ShardedLru<V> {
    private final Shard<V>[] shards;
    private final BiConsumer<String, V> onEvict;

    private static final class Shard<V> {
        final ReentrantLock lock = new ReentrantLock();
        final LinkedHashMap<String, V> map;
        Shard(int cap, ShardedLru<V> owner) {
            map = new LinkedHashMap<>(16, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<String, V> e) {
                    if (size() > cap) {
                        if (owner.onEvict != null) owner.onEvict.accept(e.getKey(), e.getValue());
                        return true;
                    }
                    return false;
                }
            };
        }
    }

    @SuppressWarnings("unchecked")
    public ShardedLru(int capacity, int shardCount, BiConsumer<String, V> onEvict) {
        this.onEvict = onEvict;
        this.shards = new Shard[shardCount];
        int per = Math.max(1, capacity / shardCount);
        for (int i = 0; i < shardCount; i++) shards[i] = new Shard<>(per, this);
    }

    private Shard<V> shard(String key) { return shards[(key.hashCode() & 0x7fffffff) % shards.length]; }

    public V get(String key) {
        Shard<V> s = shard(key);
        s.lock.lock();
        try { return s.map.get(key); } finally { s.lock.unlock(); }
    }

    public void put(String key, V v) {
        Shard<V> s = shard(key);
        s.lock.lock();
        try { s.map.put(key, v); } finally { s.lock.unlock(); }
    }

    public V getOrCreate(String key, Supplier<V> make) {
        Shard<V> s = shard(key);
        s.lock.lock();
        try {
            V v = s.map.get(key);
            if (v == null) { v = make.get(); s.map.put(key, v); }
            return v;
        } finally { s.lock.unlock(); }
    }

    public int size() {
        int n = 0;
        for (Shard<V> s : shards) { s.lock.lock(); try { n += s.map.size(); } finally { s.lock.unlock(); } }
        return n;
    }

    /** Snapshot of all values (callers lock the values themselves). */
    public List<V> values() {
        List<V> out = new ArrayList<>();
        for (Shard<V> s : shards) { s.lock.lock(); try { out.addAll(s.map.values()); } finally { s.lock.unlock(); } }
        return out;
    }
}
