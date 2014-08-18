package com.banyan.data.customtype;

import TreeStor.storage.ObjectInputUtil;

import java.nio.ByteBuffer;

/**
 * Created by nvenkataraman on 8/12/14.
 */
public class LazyString implements IBSerializable {
    Object ref;

    public LazyString()
    {
        ref=null;
    }

    public LazyString(String s)
    {
        if(s!=null)
        {
            ref=s;
        }
    }

    public LazyString(ByteBuffer buffer)
    {
        int len = ObjectInputUtil.readCInt(buffer);

        if(len!=0) {
            byte[] b = new byte[len];
            buffer.get(b);
            ref = b;
        }
        else
        {
            ref=null;
        }
    }

    public void setStringValue(String s)
    {
        ref = s;
    }

    public String getStringValue()
    {
        if(ref instanceof byte[])
        {
          ref=convertBytesToString();
        }
        return (String)ref;
    }

    @Override
    public byte getType()
    {
        return 1;
    }

    @Override
    public int getByteSize()
    {
        String str=getStringValue();
        int size=0;
        for (int i=0; i<str.length(); i++) {
            final char c = str.charAt(i);
            size+=1;
            if ( c >= 255) {
                size+=2;
            }
        }
        return size;
    }

    public int getLength()
    {
        return getStringValue().length();
    }

    @Override
    public byte[] getBytes()
    {
        if(ref instanceof byte[])
        {
            return (byte[])ref;
        }
        else
        {
            String str = (String) ref;
            byte[] b = new byte[getByteSize()];
            int bCount = 0;
            for (int i = 0; i < str.length(); i++) {
                final char c = str.charAt(i);
                b[bCount++] = (byte) c;
                if (c >= 255) {
                    // we are assigning 255 to the already assigned space.
                    b[bCount - 1] = (byte) 255;
                    b[bCount++] = (byte) ((c >>> 8) & 0xFF);
                    b[bCount++] = (byte) ((c >>> 0) & 0xFF);
                }
            }
            ref = b;
            return b;
        }
    }

    private String  convertBytesToString()
    {
        byte[] b=(byte[])ref;
        char[] charBuf = new char[b.length];

        int bCount = 0;
        int chCount = 0;
        while (bCount < b.length) {
            char head =(char)( (b[bCount++] + 256) & 0xff);
            if(head==0)
            {
                break;
            }
            if (head < 255) {
                charBuf[chCount++] = head;
            } else {
                int ch1 = ((b[bCount++] + 256) & 0xff);
                int ch2 = ((b[bCount++] + 256) & 0xff);
                charBuf[chCount++] = (char) ((ch1 << 8) + (ch2 << 0));
            }
        }
        return new String(charBuf,0,chCount);
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof String)
        {
            return o.equals(this.getStringValue());
        }
        else if(o instanceof byte[])
        {
            return ((byte[]) o).equals(this.getBytes());
        }
        else
        {
            return ((LazyString)o).getStringValue().equals(this.getStringValue());
        }
    }

    @Override
    public int hashCode()
    {
        return this.getStringValue().hashCode();
    }

    @Override
    public String toString()
    {
        return this.getStringValue();
    }
}
