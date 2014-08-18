package com.banyan.data.serialization;

import com.banyan.data.customtype.LazyString;

import java.nio.ByteBuffer;

/**
 * Created by nvenkataraman on 8/13/14.
 */
public class ObjectInputUtil
{
    public static final int readCInt(ByteBuffer buffer)
    {
        final byte head = buffer.get();
        // -128 = short byte, -127 == 4 byte
        if (head > -127 && head <= 127) {
            return head;
        }
        if (head == -128) {
            return buffer.getShort();
        } else {
            return buffer.getInt();
        }
    }

    public static long readCLong(ByteBuffer buffer)
    {
        byte head = buffer.get();
        // -128 = short byte, -127 == 4 byte
        if (head > -126 && head <= 127) {
            return head;
        }
        if (head == -128) {
            return buffer.getShort();
        } else if (head == -127) {
            return buffer.getInt();
        } else {
            return buffer.getLong();
        }
    }


    public static double readCDouble(ByteBuffer buffer)
    {
        return Double.longBitsToDouble(readCLong(buffer));
    }

    public static float readCFloat(ByteBuffer buffer)  {
        return Float.intBitsToFloat(readCInt(buffer));
    }


    public static String readCString(ByteBuffer buffer) {
        LazyString lz=new LazyString(buffer);
        return lz.getStringValue();
    }
}
