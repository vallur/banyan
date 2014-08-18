package com.banyan.data.serialization;


import com.banyan.data.customtype.LazyString;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by nvenkataraman on 8/10/14.
 */
public class ObjectOutputUtil
{
    public static void writeCInt(ByteBuffer buffer,int anInt)
    {
        // -128 = short byte, -127 == 4 byte
        if ( anInt > -127 && anInt <=127 ) {
            buffer.put((byte)anInt);
        } else
        if ( anInt >= Short.MIN_VALUE && anInt <= Short.MAX_VALUE ) {
            buffer.put((byte)-128);
            buffer.putShort((short)anInt);
        } else {
            buffer.put((byte)-127);
            buffer.putInt(anInt);
        }
    }

    /** Writes a 4 byte float. */
    public static void writeCFloat (ByteBuffer buffer,float value)
    {
        writeCInt(buffer, Float.floatToIntBits(value));
    }

    /** Writes a 4 byte float. */
    public static void writeFFloat (ByteBuffer buffer,float value) throws IOException {
        buffer.putInt(Float.floatToIntBits(value));
    }
    public static void writeCDouble (ByteBuffer buffer,double value) {
        writeCLong(buffer,Double.doubleToLongBits(value));
    }

    public static void writeCLong(ByteBuffer buffer,long anInt) {
// -128 = short byte, -127 == 4 byte
        if ( anInt > -126 && anInt <=127 ) {
            buffer.put((byte)anInt);
        } else
        if ( anInt >= Short.MIN_VALUE && anInt <= Short.MAX_VALUE ) {
            buffer.put((byte)-128);
            buffer.putShort((short)anInt);
        } else if ( anInt >= Integer.MIN_VALUE && anInt <= Integer.MAX_VALUE ) {
            buffer.put((byte)-127);
            buffer.putInt((int)anInt);
        } else {
            buffer.put((byte)-126);
            buffer.putLong(anInt);
        }
    }

    public static void writeStringUTF(ByteBuffer buffer,String str)
    {
        LazyString lz=new LazyString(str);
        byte[] b=lz.getBytes();
        writeCInt(buffer,b.length);
        buffer.put(b);
        /*for (int i=0; i<strlen; i++) {
            final char c = str.charAt(i);
            if ( c >= 255) {
                buffer.put((byte)255);
                buffer.put((byte) ((c >>> 8) & 0xFF));
                buffer.put((byte) ((c >>> 0) & 0xFF));
            }
            else
            {
                buffer.put((byte) c);
            }
        }*/
    }
}
