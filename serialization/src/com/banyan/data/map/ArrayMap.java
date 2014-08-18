package com.banyan.data.map;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by nvenkataraman on 7/6/14.
 * using fly weight pattern here is useless.
 * DataStor implementation with archive functionality
 *
 * Copyright (c) 2014 w3force All rights reserved.
 *
 * @Author  <mailto:vallur@gmail.com>Narasimhan Vallur</mailto>
 */

public class ArrayMap<K,V> extends AbstractMap<K, V> implements Serializable
{

	/**
	 * Determines if a de-serialized file is compatible with this class.
	 *
	 * Maintainers must change this value if and only if the new version
	 * of this class is not compatible with old versions. See Sun docs
	 * for <a href=http://java.sun.com/products/jdk/1.1/docs/guide
	 * /serialization/spec/version.doc.html> details. </a>
	 *
	 * Not necessary to include in first version of the class, but
	 * included here as a reminder of its importance.
	 */
	private static final long serialVersionUID = 919494293668613123L;

    public int m_bucketSize = 10;

    public int totalArraySize = 0;
    public int m_size=0;
    List<MapEntry<K, V>>[] table = null;
    Set<Map.Entry<K,V>> entrySet = null;
    Set<K> m_keySet = null;
    Collection<V> m_values = null;
    boolean reBalancingInProgress = false;
    int m_oldBucketSize = 0;

    public ArrayMap()
    {
        super();
        totalArraySize = m_bucketSize;
        table = new LinkedList[totalArraySize];
    }

    public ArrayMap(int initialSize)
    {
        super();
        //why cube root here ? My original design i was thinking of having a three dimensional array
        m_bucketSize = initialSize;

        totalArraySize = initialSize;

        table = new LinkedList[totalArraySize];

    }

    private V getBasedOnBucketSize(Object key, int bucketSize)
    {
        int bucket = (key.hashCode() & 0x7fffffff) % (bucketSize) ;

        MapEntry<K, V> entry = null;
        List<MapEntry<K, V>> list;

        if((list = table[bucket])!=null)
        {
            for (MapEntry<K,V> itemEntry:list)
            {
                if (itemEntry.getKey().equals(key))
                {
                    return itemEntry.getValue();
                }
            }
        }
        return null;
    }

    @Override
    public V get(Object key)
    {
        V value = getBasedOnBucketSize(key,m_bucketSize);
        if(value==null && reBalancingInProgress)
        {
            value = getBasedOnBucketSize(key,m_oldBucketSize);
        }
        return value;
    }

    private synchronized List<MapEntry<K,V>> getBucketDataOrDefault(int bucket)
    {
        if(table[bucket]==null)
        {
            return (table[bucket]= new LinkedList<MapEntry<K, V>>());
        }

        return table[bucket];
    }

    @Override
    public V put(K key, V value)
    {
        int bucket = getBucket(key);

        MapEntry<K, V> entry = new MapEntry<K,V>(key, value);
        List<MapEntry<K, V>> list=getBucketDataOrDefault(bucket);
        if (list.size()>0) {
            for (MapEntry<K, V> itemEntry : list) {
                if (itemEntry.getKey().equals(key)) {
                    V tempValue = itemEntry.getValue();
                    itemEntry.setValue(entry.getValue());
                    return tempValue;
                }
            }
        }
        // entry not found in the list
        list.add(entry);
        m_size++;

        return value;
    }

    @Override
    public V remove(Object key)
    {
        int hash = getBucket(key);
        int firstIndex = (hash) / m_bucketSize;

        MapEntry<K,V> entry;
        List<MapEntry<K, V>> list;
        if (table[firstIndex] != null)
        {
            list = table[firstIndex];
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).equals(key)) {
                    m_size--;
                    return list.remove(i).getValue();
                }
            }
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        for(Map.Entry entry:m.entrySet())
        {
            put((K)entry.getKey(),(V)entry.getValue());
        }
    }

    public void rebalance(int newBucketSize)
    {
        List<MapEntry<K,V>>[] swapTable = new LinkedList[newBucketSize];

         if(newBucketSize > m_bucketSize)
         {
             System.arraycopy(table, 0, swapTable, 0,
                     Math.min(table.length, newBucketSize));

             reBalancingInProgress = true;

             synchronized (this)
             {
                 table = swapTable;
             }
         }

        synchronized (this)
        {
            m_oldBucketSize = m_bucketSize;
            //second step mark the actual bucket size to be newBucketSize
            m_bucketSize = newBucketSize;
        }

        //The number of elements in the array does not change so we dont need to change the count.

        //The bucket size should be a prime number to avoid the same key to be allocated to same bucket even
        // if the bucketsize changes. We dont want the database to be on a non queryable state  so we will do
        // this operation in steps
        // first step rebalance the keys
        for(Map.Entry<K,V> entry:this.entrySet())
        {
            int newBucket=(entry.getKey().hashCode() & 0x7fffffff) % (newBucketSize);
            int oldBucket=(entry.getKey().hashCode() & 0x7fffffff) % (m_oldBucketSize);
            if(newBucket!=oldBucket)
            {
                // we know for sure this element is not part of the new bucket.
                List<MapEntry<K, V>> list = table[newBucket];
                if (list == null)
                {
                    synchronized (table)
                    {
                        list = new LinkedList<MapEntry<K, V>>();

                        table[newBucket] = list;
                    }
                }

                list.add(new MapEntry<K, V>(entry.getKey(), entry.getValue()));
            }
        }

        //third step remove the keys from the old bucket
        //The keys from the old bucket have to be removed as they would still show up
        // if we try to get all keys from the collection there could be duplicates.
        // To avoid duplicates use prime number of buckets. There will not be duplicates in
        // range queries.
        for(Map.Entry<K,V> entry:this.entrySet())
        {
            int newBucket=(entry.getKey().hashCode() & 0x7fffffff) % (newBucketSize);
            int oldBucket=(entry.getKey().hashCode() & 0x7fffffff) % (m_oldBucketSize);
            if(newBucket!=oldBucket)
            {
                List<MapEntry<K, V>> list = table[oldBucket];
                synchronized (table)
                {
                   for(int i=0;i<list.size();i++)
                   {
                       if(list.get(i).getKey().equals(entry.getKey()))
                       {
                           list.remove(i);
                           break;
                       }
                   }
                }
            }
        }

        //reset values
        m_oldBucketSize=m_bucketSize;
        reBalancingInProgress = false;
    }

    @Override
    public void clear()
    {

    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     *
     * @return a set view of the keys contained in this map
     */
    @Override
    public Set<K> keySet() {
        Set<K> ks;
        return (ks = m_keySet) == null ? (m_keySet = new KeySet()) : ks;
    }

    final class KeySet extends AbstractSet<K> {
        public final int size()                 { return m_size; }
        public final void clear()               {  }
        public final Iterator<K> iterator()     { return new KeyIterator(); }
        public final boolean contains(Object o) { return containsKey(o); }
        public final boolean remove(Object key) {
            return false;//removeNode(hash(key), key, null, false, true) != null;
        }
        public final Spliterator<K> spliterator()
        {
            return null;//return new KeySpliterator<>(HashMap.this, 0, -1, 0, 0);
        }
        public final void forEach(Consumer<? super K> action) {

            MapEntry<K,V>[] tab;
            if (action == null)
                throw new NullPointerException();
            if (m_size > 0 ) {
                int mc = m_bucketSize;
                for (int i = 0; i < table.length; ++i)
                {
                    if(table[i]!=null)
                    {
                        for (int k = 0; k < table[i].size(); k++)
                        {
                            action.accept(table[i].get(k).getKey());
                        }
                    }
                }
                if (m_bucketSize != mc)
                    throw new ConcurrentModificationException();
            }
        }
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a view of the values contained in this map
     */
    public Collection<V> values() {
        Collection<V> vs;
        return (vs = m_values) == null ? (m_values = new Values()) : vs;
    }

    final class Values extends AbstractCollection<V> {
        public final int size()                 { return m_size; }
        public final void clear()               { }//HashMap.this.clear(); }
        public final Iterator<V> iterator()     { return new ValueIterator(); }
        public final boolean contains(Object o) { return containsValue(o); }
        public final Spliterator<V> spliterator() {return null;}
        //return new ValueSpliterator<>(HashMap.this, 0, -1, 0, 0);

        public final void forEach(Consumer<? super V> action) {
            MapEntry<K,V>[] tab;
            if (action == null)
                throw new NullPointerException();
            if (m_size > 0 ) {
                int mc = m_bucketSize;
                for (int i = 0; i < table.length; ++i)
                {
                    if(table[i]!=null)
                    {
                        for (int j = 0; j < table[i].size(); j++)
                        {
                            action.accept(table[i].get(j).getValue());
                        }
                    }
                }
                if (m_bucketSize != mc)
                    throw new ConcurrentModificationException();
            }
        }
    }
    @Override
    public Set<Map.Entry<K,V>> entrySet() {
        Set<Map.Entry<K,V>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
    }

    final class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        public final int size()                 { return m_size; }
        public final void clear()               { this.clear(); }
        public final Iterator<Map.Entry<K,V>> iterator() {
            return new EntryIterator();
        }
        public final boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>) o;
            Object key = e.getKey();
            return get(key) != null;
        }
        public final boolean remove(Object o) {
            return false;
        }
        public final Spliterator<Map.Entry<K,V>> spliterator() {
            return null;
        }
        public final void forEach(Consumer<? super Entry<K,V>> action) {
            if (action == null)
                throw new NullPointerException();
            if (m_size > 0 ) {
                int mc = m_bucketSize;
                for (int i = 0; i < table.length; ++i)
                {
                    if(table[i]!=null)
                    {
                        for (int j = 0; j < table[i].size(); j++) {
                            action.accept(table[i].get(j));
                        }
                    }
                }
                if (m_bucketSize != mc)
                    throw new ConcurrentModificationException();
            }
        }
    }

    /* ------------------------------------------------------------ */
    // iterators

    abstract class HashIterator {
        int expectedModCount;  // for fast-fail
        int index;             // current slot

        int bucket;
        int listIndex;

        HashIterator()
        {
            expectedModCount = m_bucketSize;

            index = 0;
            bucket = 0;
            listIndex = 0;
        }

        public final boolean hasNext() {
            return index < m_size;
        }

        final Entry<K,V> nextNode()
        {
            MapEntry<K,V> e = null;
            if (m_bucketSize != expectedModCount)
                throw new ConcurrentModificationException();

            if(table[bucket]!=null && listIndex<table[bucket].size())
            {
                index++;
                return table[bucket].get(listIndex++);
            }
            else
            {
                listIndex = 0;
                for(bucket++;bucket < table.length;bucket++)
                {
                    if(table[bucket]!=null)
                    {
                        try
                        {
                            index++;
                            return table[bucket].get(listIndex++);
                        }
                        catch(Exception ex)
                        {
                            ex.printStackTrace();
                        }
                    }
                }
            }
            if (e == null)
                throw new NoSuchElementException();

            return e;
        }

        public final void remove()
        {

        }
    }

    final class EntryIterator extends HashIterator
            implements Iterator<Map.Entry<K,V>>
    {
        public final Map.Entry<K,V> next() { return nextNode(); }
    }

    final class KeyIterator extends HashIterator
            implements Iterator<K>
    {
        public final K next() { return nextNode().getKey(); }
    }

    final class ValueIterator extends HashIterator
            implements Iterator<V>
    {
        public final V next() { return nextNode().getValue(); }
    }

    @Override
    public int size()
    {
        return m_size;
    }

    @Override
    public boolean isEmpty()
    {
        return m_size==0;
    }

    @Override
    public boolean containsKey(Object key)
    {
        return (get(key)!=null);
    }

    @Override
    public boolean containsValue(Object value)
    {
        // not supported
        return false;
    }


    static final int hash(Object key)
    {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }


    private int getBucket(Object key)
    {
        return (key == null) ? 0 : (key.hashCode() & 0x7fffffff) % (totalArraySize);
    }


    //2,198,400,000
    //2198,400
    //2198

    //1000*3
    //10,000,000,000

}
