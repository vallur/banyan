/**
 * Copyright 2011 Expedia, Inc. All rights reserved.
 * EXPEDIA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.banyan.data;

import com.banyan.data.customtype.IBSerializable;
import com.banyan.data.type.LazyString;

import java.io.Serializable;
import java.nio.ByteBuffer;


/**
 * DataStor implementation with archive functionality
 *
 * Copyright (c) 2014 w3force All rights reserved.
 *
 * @Author  <mailto:vallur@gmail.com>Narasimhan Vallur</mailto>
 */
public class TDataKey implements Serializable, IBSerializable
{
	private TDataEnum	    m_dataType;
	private String 		m_columnName;
    transient private int   m_index;

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
    private static final long serialVersionUID = 4299725429774212217L;

    public TDataKey(ByteBuffer buffer)
    {
        m_dataType = TDataEnum.values()[buffer.get()];
        //m_columnName = new LazyString(buffer);
    }

	public TDataKey(LazyString columnName,TDataEnum dataType)
	{
		m_dataType = dataType;
		//m_columnName = columnName;
	}

    public TDataKey(String columnName,TDataEnum dataType)
    {
        m_dataType = dataType;
        m_columnName = columnName;
        //m_columnName = new LazyString();
        //m_columnName.setStringValue(columnName);
    }

	public TDataEnum getDataType()
	{
		return m_dataType;
	}

	public void setDataType(TDataEnum dataType)
	{
		m_dataType = dataType;
	}

	public String getColumnName()
	{
		return m_columnName;
	}

	public void setColumnName(String columnName)
	{
		//m_columnName.setStringValue(columnName);
	}

    public int getColumnIndex()
    {
        return m_index;
    }

    public void setColumnIndex(int index)
    {
        m_index=index;
    }

    @Override
    public byte[] getBytes() {

        return new byte[0];
    }

    @Override
    public byte getType() {
        return 125;
    }

    @Override
    public int getByteSize()
    {
        //Type + columnName
        return 2 + 5;//(m_columnName.getByteSize());
    }
}
