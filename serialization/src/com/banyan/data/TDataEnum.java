package com.banyan.data;

import com.banyan.data.customtype.IBSerializable;
import com.banyan.data.customtype.LazyString;
import com.banyan.serialization.ObjectInputUtil;
import com.banyan.serialization.ObjectOutputUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataStor implementation with archive functionality
 *
 * Copyright (c) 2014 w3force All rights reserved.
 *
 * @Author  <mailto:vallur@gmail.com>Narasimhan Vallur</mailto>
 */
public enum TDataEnum
{
	STRING
            {
                @Override
                public int getByteSize(Object s,int level)
                {
                    return (((String)s).length()*3)+6;
                }

                @Override
                public void write(ByteBuffer buffer, Object o,int level)
                {
                    buffer.put((byte)LAZYSTRING.ordinal());
                    ObjectOutputUtil.writeStringUTF(buffer,(String)o);
                }

                @Override
                public Object read(ByteBuffer buffer)
                {
                    return ObjectInputUtil.readCString(buffer);
                }
            },
	INT{
        @Override
        public int getByteSize(Object o,int level)
        {
            return 6;
        }

        @Override
        public void write(ByteBuffer buffer, Object o,int level)
        {
            buffer.put((byte)TDataEnum.INT.ordinal());
            ObjectOutputUtil.writeCInt(buffer,(int)o);
        }

        @Override
        public Object read(ByteBuffer buffer)
        {
            return ObjectInputUtil.readCInt(buffer);
        }
    },
	DOUBLE
            {
        @Override
        public int getByteSize(Object d,int level) {
            return 11;
        }

        @Override
        public void write(ByteBuffer buffer, Object o,int level)
        {
            buffer.put((byte)this.ordinal());
            ObjectOutputUtil.writeCDouble(buffer, (double)o);
        }

        @Override
        public Object read(ByteBuffer buffer)
        {
             return ObjectInputUtil.readCDouble(buffer);
        }
            },
    LAZYSTRING
            {
                @Override
                public int getByteSize(Object s,int level) {
                    return ((LazyString)s).getByteSize()+6;
                }

                @Override
                public Object read(ByteBuffer buffer)
                {
                    return new LazyString(buffer);
                }
            },
	FLOAT
        {
            @Override
        public int getByteSize(Object o,int level)
        {
            return 6;
        }

            @Override
        public void write(ByteBuffer buffer, Object o,int level)
        {
            buffer.put((byte)TDataEnum.FLOAT.ordinal());
            ObjectOutputUtil.writeCFloat(buffer, (float)o);
        }

        @Override
        public Object read(ByteBuffer buffer)
        {
            return ObjectInputUtil.readCFloat(buffer);
        }
        },
	DATE
        {
            @Override
        public int getByteSize(Object o,int level) // Dates in banyan are stored as long though type is different
        {
            return 11;
        }


            @Override
            public void write(ByteBuffer buffer, Object l,int level)
            {
                buffer.put((byte)this.ordinal());
                ObjectOutputUtil.writeCLong(buffer,(Long)l);
            }

            @Override
            public Object read(ByteBuffer buffer) {
                return ObjectInputUtil.readCLong(buffer);
            }
        },
    BIGDECIMAL
            {
                @Override
                public Object read(ByteBuffer buffer)
                {
                    byte length=buffer.get();
                    byte[] b=new byte[length];
                    buffer.get(b);
                    return new BigDecimal(new BigInteger(b));
                }

                @Override
                public int getByteSize(Object o, int level) {
                    return 20;
                }

                @Override
                public void write(ByteBuffer buffer, Object l,int level)
                {
                    buffer.put((byte)TDataEnum.BIGDECIMAL.ordinal());
                    BigDecimal bd=(BigDecimal)l;
                    byte[] b=bd.toBigInteger().toByteArray();
                    buffer.put((byte)b.length);
                    buffer.put(b);
                }
            },
    BIGINTEGER
            {
                @Override
                public Object read(ByteBuffer buffer)
                {
                    byte length=buffer.get();
                    byte[] b=new byte[length];
                    buffer.get(b);
                    return new BigInteger(b);
                }

                @Override
                public int getByteSize(Object o, int level) {
                    return 20;
                }

                @Override
                public void write(ByteBuffer buffer, Object l,int level)
                {
                    buffer.put((byte)TDataEnum.BIGINTEGER.ordinal());
                    BigInteger bi=(BigInteger)l;
                    byte[] b=bi.toByteArray();
                    buffer.put((byte)b.length);
                    buffer.put(b);
                }
            },
	LONG{
        @Override
        public int getByteSize(Object o,int level) // Dates in banyan are stored as long though type is different
        {
            return 11;
        }

        @Override
        public void write(ByteBuffer buffer, Object l,int level)
        {
            buffer.put((byte)TDataEnum.LONG.ordinal());
            ObjectOutputUtil.writeCLong(buffer,(Long)l);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return ObjectInputUtil.readCLong(buffer);
        }
    },
	SHORT{
        @Override
        public int getByteSize(Object o,int level) // Dates in banyan are stored as long though type is different
        {
            return 4;
        }

        public void write(ByteBuffer buffer, Object s,int level)
        {
            buffer.put((byte)TDataEnum.SHORT.ordinal());
            buffer.putShort((Short)s);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getShort();
        }
    },
	BOOLEAN{
        @Override
        public int getByteSize(Object o,int level)
        {
            return 2;
        }

        @Override
        public void write(ByteBuffer buffer, Object b,int level)
        {
            buffer.put((byte)TDataEnum.BOOLEAN.ordinal());
            buffer.put((byte) ((boolean)b ? 0 : 1));
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.get()==0?true:false;
        }
    },
    MapType {
        @Override
        public int getByteSize(Object map,int level) {
            int size=1+5;
            if(level==6)
            {
                return 0;
            }
            level++;
            for(Map.Entry<Object,Object> entry:((Map<Object,Object>)map).entrySet())
            {
                size+=OBJECT.getByteSize(entry.getKey(),level);
                size+=OBJECT.getByteSize(entry.getValue(),level);
            }
            return size;
        }

        @Override
        public void write(ByteBuffer buffer, Object o,int level)
        {
            if(level==6)
            {
                return;
            }

            level++;
            buffer.put((byte)this.ordinal());
            Map<Object,Object> map=(Map<Object,Object>)o;
            ObjectOutputUtil.writeCInt(buffer,map.size());
            for(Map.Entry<Object,Object> entry:map.entrySet())
            {
                OBJECT.write(buffer,entry.getKey(),level);
                OBJECT.write(buffer,entry.getValue(),level);
            }
        }

        @Override
        public Object read(ByteBuffer buffer)
        {
            int len = ObjectInputUtil.readCInt(buffer);
            Map<Object,Object> map=new HashMap<>(len);
            for(int i=0;i<len;i++)
            {
                int type=buffer.get();
                Object key=TDataEnum.values()[type].read(buffer);
                type=buffer.get();
                Object value=TDataEnum.values()[type].read(buffer);
                map.put(key,value);
            }
               return map;
        }
    },
    ListType
     {  @Override
        public int getByteSize(Object l,int level)
        {
            int size=1; // for single byte type value
            size+=5;
            List list = (List)l;
            // do not use iterator if you do null items will be ignored.
            for(int i=0;i<list.size();i++)
            {
                size+=OBJECT.getByteSize(list.get(i),level);

            }
            return size;

        }

         @Override
        public void write(ByteBuffer buffer, Object l,int level)
        {
            buffer.put((byte)this.ordinal());
            List list=(List)l;
            int length=list.size();
            ObjectOutputUtil.writeCInt(buffer, length);
            // do not use iterator if you do null items will be ignored.
            for(int i=0;i<length;i++)
            {
                OBJECT.write(buffer,list.get(i),level);
            }
        }

         @Override
         public Object read(ByteBuffer buffer)
         {
             int type;
             int size = ObjectInputUtil.readCInt(buffer);
             if(size>0) {
                 List list = new ArrayList<>(size);

                 for (int i = 0; i < size; i++)
                 {
                     type = buffer.get();
                     list.add(TDataEnum.values()[type].read(buffer));
                 }
                 return list;
             }
             return null;
         }
     },
    BYTEARRAY
            {
        @Override
        public int getByteSize(Object b,int level)
        {
            return 6+((byte[])b).length;
        }

        @Override
        public void write(ByteBuffer buffer, Object o,int level)
        {
            buffer.put((byte)this.ordinal());
            byte[] b=(byte[])o;
            int length=0;
            if(b.length>0)
            {
                length = b.length;
            }
            ObjectOutputUtil.writeCInt(buffer, b.length);
            buffer.put(b);
        }

        @Override
        public Object read(ByteBuffer buffer)
        {
            int length = ObjectInputUtil.readCInt(buffer);
            byte[] b=new byte[length];
            buffer.get(b);
            return b;
        }
            },
    IBSERIALIZABLE{
        @Override
        public int getByteSize(Object serializable,int level)
        {
            return ((IBSerializable)serializable).getByteSize();
        }

        @Override
        public Object read(ByteBuffer buffer)
        {
              return null;
        }
    },
    TDATATABLE
            {   @Override
                public int getByteSize(Object serializable,int level)
                {

                    if(level==6)
                    {
                        return 0;
                    }
                    level++;
                    TDataTable table = (TDataTable) serializable;
                    return MapType.getByteSize(((TDataTable) serializable).rowMap(),level);
                }

                @Override
                public void write(ByteBuffer buffer,Object o,int level)
                {
                    if(level==6)
                    {
                        return;
                    }
                    level++;
                    TDataTable table=(TDataTable)o;
                        buffer.put((byte) this.ordinal());
                        Map<Object, Object> map = table.rowMap();
                        ObjectOutputUtil.writeCInt(buffer, map.size());
                        for (Map.Entry<Object, Object> entry : map.entrySet()) {
                            OBJECT.write(buffer, entry.getKey(),level);
                            OBJECT.write(buffer, entry.getValue(),level);
                        }
                }

                @Override
                public Object read(ByteBuffer buffer)
                {
                    TDataTable table=new TDataTable();
                    table.setRowMap((Map) MapType.read(buffer));
                    return table;
                }
            },
    OBJECT
            {
                @Override
                public int getByteSize(Object o,int level)
                {
                    if(o == null)
                    {
                        return 0;
                    }
                    Class cl = o.getClass();
                    return classMap.get(cl.getName()).getByteSize(o,level);
                }

                @Override
                public Object read(ByteBuffer buffer)
                {
                    int type=buffer.get();
                    return TDataEnum.values()[type].read(buffer);
                }

                @Override
                public void write(ByteBuffer buffer,Object o,int level)
                {
                    if(o==null)
                    {
                        NULL.write(buffer,null,0);
                        return;
                    }
                    Class cl = o.getClass();
                    classMap.get(cl.getName()).write(buffer,o,level);
                }
            },
    NULL {
        @Override
        public Object read(ByteBuffer buffer) {
            return null;
        }

        @Override
        public int getByteSize(Object o, int level) {
            return 1;
        }

    },
    TDATAKEY
            {   @Override
                public int getByteSize(Object serializable,int level)
                {
                    return ((IBSerializable)serializable).getByteSize();
                }

                @Override
                public Object read(ByteBuffer buffer)
                {
                     return new TDataKey(buffer);
                }

            },
    BYTE{
        @Override
        public Object read(ByteBuffer buffer)
        {
            return buffer.get();
        }

        @Override
        public int getByteSize(Object o,int level)
        {
            return 2;
        }

        @Override
        public void write(ByteBuffer buffer, Object o,int level)
        {
            buffer.put((byte)this.ordinal());
            buffer.put((byte)o);
        }
    }
    ;

    public abstract Object read(ByteBuffer buffer);
    public abstract int getByteSize(Object o,int level);

    public void write(ByteBuffer buffer, Object o,int level)
    {
        buffer.put((byte) this.ordinal()); // 1 stands for lazystring
        if(o!=null)
        {
            IBSerializable instance = (IBSerializable) o;
            byte[] b = instance.getBytes();
            int length = 0;
            if (b != null && b.length > 0) {
                length = b.length;
            }
            ObjectOutputUtil.writeCInt(buffer, length);
            buffer.put(b);
        }
    }

    public static Map<String,TDataEnum> classMap = new HashMap<>();

    static {
        classMap.put(TDataTable.class.getName(), TDataEnum.TDATATABLE);
        classMap.put(TDataKey.class.getName(), TDataEnum.TDATAKEY);
        classMap.put(ArrayList.class.getName(), TDataEnum.ListType);
        classMap.put(ArrayMap.class.getName(), TDataEnum.MapType);
        classMap.put(HashMap.class.getName(), TDataEnum.MapType);
        classMap.put(ConcurrentHashMap.class.getName(), TDataEnum.MapType);
        classMap.put(Integer.class.getName(), TDataEnum.INT);
        classMap.put(Long.class.getName(), TDataEnum.LONG);
        classMap.put(Double.class.getName(), TDataEnum.DOUBLE);
        classMap.put(Float.class.getName(), TDataEnum.FLOAT);
        classMap.put(Byte.class.getName(), TDataEnum.BYTE);
        classMap.put(String.class.getName(), TDataEnum.STRING);
        classMap.put(Boolean.class.getName(), TDataEnum.BOOLEAN);
        classMap.put(byte[].class.getName(), TDataEnum.BYTEARRAY);
        classMap.put(LazyString.class.getName(), TDataEnum.LAZYSTRING);
        classMap.put(BigDecimal.class.getName(), TDataEnum.BIGDECIMAL);
        classMap.put(BigInteger.class.getName(), TDataEnum.BIGINTEGER);
    }

}
