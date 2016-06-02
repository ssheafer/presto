package com.facebook.presto.spi.block;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

public class DynamicByteArray
{
    private byte[] bytes;
    private int size;
    private Slice underlyingSlice;

    public DynamicByteArray(int initialCapacity)
    {
        bytes = new byte[initialCapacity];
        underlyingSlice = Slices.wrappedBuffer(bytes);
    }

    public void appendByte(byte value)
    {
        resizeIfNecessary(1);
        bytes[size] = value;
        size++;
    }

    public void appendBytes(Slice source, int sourceIndex, int length)
    {
        resizeIfNecessary(length);
        source.getBytes(sourceIndex, bytes, size, length);
        size += length;
    }

    public void resizeIfNecessary(int length)
    {
        if (size + length >= bytes.length) {
            bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, size + length));
            underlyingSlice = Slices.wrappedBuffer(bytes);
        }
    }

    public byte get(int position)
    {
        if (position >= size) {
            throw new IllegalArgumentException();
        }
        return bytes[position];
    }

    public boolean equals(int offset, byte[] other, int otherOffset, int length)
    {
        if (offset + length > size) {
            return false;
        }
        if (bytes == other && offset == otherOffset) {
            return true;
        }

        for (int i = 0; i < length; i++) {
            if (bytes[offset + i] != other[otherOffset + i]) {
                return false;
            }
        }

        return true;
    }

    public int size()
    {
        return size;
    }

    public int getRetainedSize()
    {
        return (int) SizeOf.sizeOf(bytes) + SizeOf.SIZE_OF_INT;
    }

    public Slice getUnderlyingSlice()
    {
        return underlyingSlice;
    }

    public Slice slice()
    {
        return Slices.wrappedBuffer(bytes, 0, size);
    }
}
