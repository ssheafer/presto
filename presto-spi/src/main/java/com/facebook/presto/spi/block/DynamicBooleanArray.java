/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.block;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

public class DynamicBooleanArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DynamicBooleanArray.class).instanceSize();

    private boolean[] values;
    private int size;
    private Slice underlyingSlice;

    private DynamicBooleanArray(boolean[] values, int size)
    {
        this.values = values;
        this.size = size;
        underlyingSlice = Slices.wrappedBooleanArray(values);
    }

    public DynamicBooleanArray(int initialCapacity)
    {
        this(new boolean[initialCapacity], 0);
    }

    public static DynamicBooleanArray wrap(boolean... values)
    {
        return new DynamicBooleanArray(values, values.length);
    }

    public DynamicBooleanArray append(boolean value)
    {
        resizeIfNecessary();
        values[size] = value;
        size++;
        return this;
    }

    private void resizeIfNecessary()
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

    public long getByteSize()
    {
        return SizeOf.sizeOf(values);
    }

    public long getRetainedByteSize()
    {
        return INSTANCE_SIZE + getByteSize();
    }

    public DynamicBooleanArray copyRegion(int position, int length)
    {
        if (position + length > size) {
            throw new IllegalArgumentException("Invalid region to copy");
        }
        return wrap(Arrays.copyOfRange(values, position, position + length));
    }

    public Slice getUnderlyingSlice()
    {
        return underlyingSlice;
    }

    public Slice slice()
    {
        return Slices.wrappedBooleanArray(values, 0, size);
    }

    @Override
    public String toString()
    {
        return "DynamicBooleanArray{" +
                "values=" + Arrays.toString(values) +
                ", size=" + size +
                '}';
    }
}
