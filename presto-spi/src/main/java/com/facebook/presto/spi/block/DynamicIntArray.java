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
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

public class DynamicIntArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DynamicIntArray.class).instanceSize();

    private int[] values;
    private int size;
    private Slice underlyingSlice;

    private DynamicIntArray(int[] values, int size)
    {
        this.values = values;
        this.size = size;
        underlyingSlice = Slices.wrappedIntArray(values);
    }

    public DynamicIntArray(int initialCapacity)
    {
        this(new int[initialCapacity], 0);
    }

    public static DynamicIntArray wrap(int... values)
    {
        return new DynamicIntArray(values, values.length);
    }

    public DynamicIntArray append(int value)
    {
        resizeIfNecessary(1);
        values[size] = value;
        size++;
        return this;
    }

    public DynamicIntArray append(SliceInput input, int positions)
    {
        resizeIfNecessary(positions);
        input.readBytes(underlyingSlice, size * Integer.BYTES, positions * Integer.BYTES);
        size += positions;
        return this;
    }

    private void resizeIfNecessary(int newMembers)
    {
        if (size + newMembers >= values.length) {
            values = Arrays.copyOf(values, Math.max(values.length * 2, size + newMembers));
            underlyingSlice = Slices.wrappedIntArray(values);
        }
    }

    public int get(int position)
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

    public DynamicIntArray copyRegion(int position, int length)
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
        return Slices.wrappedIntArray(values, 0, size);
    }

    @Override
    public String toString()
    {
        return "DynamicIntArray{" +
                "values=" + Arrays.toString(values) +
                ", size=" + size +
                '}';
    }
}
