package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultKeyValue implements KeyValue {

    private final Map<String, Object> kvs = new HashMap<>();
    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return (Integer)kvs.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return (Long)kvs.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return (Double)kvs.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String)kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }

	@Override
	public String toString() {
		return "[" + kvs + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((kvs == null) ? 0 : kvs.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultKeyValue other = (DefaultKeyValue) obj;
		if (kvs == null) {
			if (other.kvs != null)
				return false;
		} else if (!kvs.equals(other.kvs))
			return false;
		return true;
	}
	
}
