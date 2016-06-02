package com.facebook.presto.spi.block;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

public class DynamicBooleanArray
{
    private boolean[] values;
    private int size;
    private Slice underlyingSlice;

    public DynamicBooleanArray(int initialCapacity)
    {
        values = new boolean[initialCapacity];
        underlyingSlice = Slices.wrappedBooleanArray(values);
    }

    public void append(boolean value)
    {
        resizeIfNecessary();
        values[size] = value;
        size++;
    }

    public void resizeIfNecessary()
    {
        if (size >= values.length) {
            values = Arrays.copyOf(values, values.length * 2);
            underlyingSlice = Slices.wrappedBooleanArray(values);
        }
    }

    public boolean get(int position)
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
        return Slices.wrappedBooleanArray(values, 0, size);
    }
}
