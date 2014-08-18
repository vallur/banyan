package com.banyan.data.map;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by nvenkataraman on 7/6/14.
 * using fly weight pattern here is useless.
 * DataStor implementation with archive functionality
 *
 * Copyright (c) 2014 w3force All rights reserved.
 *
 * @Author  <mailto:vallur@gmail.com>Narasimhan Vallur</mailto>
 */
public class MapEntry<K,V> implements Map.Entry<K,V>, Serializable
{
    private final K key;
    private V value;
    private boolean rebalanceEntry=false;

    public boolean isInRebalance()
    {
        return rebalanceEntry;
    }

    public void setRebalanceEntry(boolean value)
    {
        rebalanceEntry = value;
    }

    public MapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }


    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public V setValue(V value) {
        this.value = value;
        return value;
    }

    @Override
    public boolean equals(Object o)
    {
        return (key.equals(o));
    }
}