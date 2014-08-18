package com.banyan.data.customtype;

import java.io.Serializable;

/**
 * Created by nvenkataraman on 8/13/14.
 */
public interface IBSerializable extends Serializable
{
    byte[] getBytes();

    byte getType();

    int getByteSize();
}
