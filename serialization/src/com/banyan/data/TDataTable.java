
package com.banyan.data;

import com.banyan.data.customtype.IBSerializable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * DataStor implementation with archive functionality
 *
 * Copyright (c) 2014 w3force All rights reserved.
 *
 * @Author  <mailto:vallur@gmail.com>Narasimhan Vallur</mailto>
 *
 * Already tried using Guava Table and that does not meet the performance requirements.
 * Memory consumption with guava table is very good
 */
public class TDataTable<T,R,C> implements Serializable, IBSerializable
{
	// row , column , value
	Map<T,Map<R,C>> tableData;

	transient boolean m_leaf=false;

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
	private static final long serialVersionUID = 72273334256794380L;

    private String m_name="";

	public TDataTable()
	{
		tableData = new HashMap<T, Map<R, C>>();
	}

	public TDataTable(boolean leaf)
	{
		tableData = new HashMap<T, Map<R, C>>();
		m_leaf = leaf;
	}

    public void loadAll(TDataTable<T,R,C> source)
    {
        tableData.putAll(source.rowMap());
    }

    public Map<R,C> row(T key)
    {
        return tableData.get(key);
    }

    public String getName()
    {
        return m_name;
    }

    public void setName(String name)
    {
        m_name = name;
    }

	public TDataTable(int initialCapacity)
	{
		tableData = new HashMap<T, Map<R, C>>(initialCapacity);
	}

	public TDataTable(int initialCapacity,boolean leaf)
	{
		tableData = new HashMap<T, Map<R, C>>(initialCapacity);
		m_leaf=leaf;
	}

    public void setRowMap(Map<T,Map<R,C>> rowMap)
    {
        tableData = rowMap;
    }

	public C put(T keys,R columnName, C values)
	{
		Map<R,C> childData = tableData.get(keys);
		if(childData==null)
		{
			childData = new HashMap<R, C>();
			tableData.put(keys, childData);
		}
		return childData.put(columnName,values);
	}

	public Map<R,C> getBranch(T row)
	{
		return tableData.get(row);
	}

	public C get(T keys, R columnName)
	{
		synchronized (this)
		{
			try
			{
				return tableData.get(keys).get(columnName);
			}
			catch (Exception ex)
			{
                System.out.println(keys.toString());
				ex.printStackTrace();
			}
		}
		return null;
	}

	public Set<T> rowKeySet()
	{
		return tableData.keySet();
	}

	public int size()
	{
		return tableData.size();
	}

	public boolean containsRow(T rowKey)
	{
		return tableData.containsKey(rowKey);
	}

	public C remove(T rowKey,R columnName)
	{
		synchronized (this)
		{
			return tableData.get(rowKey).remove(columnName);
		}
	}

    public Map<T,Map<R,C>> rowMap()
    {
        return tableData;
    }


    // convinience method use the TDataEnum instead while calling from a specific thread.
    @Override
    public byte[] getBytes()
    {
        ByteBuffer buffer = ByteBuffer.allocate(TDataEnum.TDATATABLE.getByteSize(this,0));
        TDataEnum.TDATATABLE.write(buffer,this,0);
        byte[] b=new byte[buffer.position()];
        buffer.clear();
        buffer.get(b);
        return b;
    }

    @Override
    public byte getType() {
        return 124;
    }

    @Override
    public int getByteSize()
    {
        return TDataEnum.TDATATABLE.getByteSize(this,0);
    }
}
