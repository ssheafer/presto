
package com.facebook.presto.spi.block;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

public class DynamicLongArray
{
    private long[] values;
    private int size;
    private Slice underlyingSlice;

    public DynamicLongArray(int initialCapacity)
    {
        values = new long[initialCapacity];
        underlyingSlice = Slices.wrappedLongArray(values);
    }

    public void appendLong(long value)
    {
        resizeIfNecessary();
        values[size] = value;
        size++;
    }

    public void resizeIfNecessary()
    {
        if (size >= values.length) {
            values = Arrays.copyOf(values, values.length * 2);
            underlyingSlice = Slices.wrappedLongArray(values);
        }
    }

    public long get(int position)
    {
        if (position >= size) {
            throw new IllegalArgumentException();
        }
        return values[position];
    }

    public int size()
    {
        return size;
    }

    public int getRetainedSize()
    {
        return (int) SizeOf.sizeOf(values) + SizeOf.SIZE_OF_INT;
    }

    public Slice getUnderlyingSlice()
    {
        return underlyingSlice;
    }

    public Slice slice()
    {
        return Slices.wrappedLongArray(values, 0, size);
    }
}
