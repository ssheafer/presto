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

import static com.facebook.presto.spi.block.JvmUtils.unsafe;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class DynamicByteArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DynamicByteArray.class).instanceSize();

    private byte[] bytes;
    private int size;
    private Slice underlyingSlice;

    private DynamicByteArray(byte[] bytes, int size)
    {
        this.bytes = bytes;
        this.size = size;
        underlyingSlice = Slices.wrappedBuffer(bytes);
    }

    public DynamicByteArray(int initialCapacity)
    {
        this(new byte[initialCapacity], 0);
    }

    public static DynamicByteArray wrap(byte... bytes)
    {
        return new DynamicByteArray(bytes, bytes.length);
    }

    public DynamicByteArray appendByte(byte value)
    {
        resizeIfNecessary(1);
        bytes[size] = value;
        size++;
        return this;
    }

    public DynamicByteArray appendBytes(Slice source, int sourceIndex, int length)
    {
        resizeIfNecessary(length);
        source.getBytes(sourceIndex, bytes, size, length);
        size += length;
        return this;
    }

    public DynamicByteArray appendBytes(SliceInput source, int length)
    {
        resizeIfNecessary(length);
        source.read(bytes, size, length);
        size += length;
        return this;
    }

    public DynamicByteArray appendBytes(DynamicByteArray source, int sourceIndex, int length)
    {
        resizeIfNecessary(length);
        System.arraycopy(source.bytes, sourceIndex, bytes, size, length);
        size += length;
        return this;
    }

    public DynamicByteArray appendShort(short value)
    {
        resizeIfNecessary(Short.BYTES);
        // TODO: why does slice mask the value?
        unsafe.putShort(bytes, (long) ARRAY_BYTE_BASE_OFFSET + size, value);
        size += Short.BYTES;
        return this;
    }

    public DynamicByteArray appendInt(int value)
    {
        resizeIfNecessary(Integer.BYTES);
        unsafe.putInt(bytes, (long) ARRAY_BYTE_BASE_OFFSET + size, value);
        size += Integer.BYTES;
        return this;
    }

    public DynamicByteArray appendLong(long value)
    {
        resizeIfNecessary(Long.BYTES);
        unsafe.putLong(bytes, (long) ARRAY_BYTE_BASE_OFFSET + size, value);
        size += Long.BYTES;
        return this;
    }

    public DynamicByteArray appendFloat(float value)
    {
        resizeIfNecessary(Float.BYTES);
        unsafe.putFloat(bytes, (long) ARRAY_BYTE_BASE_OFFSET + size, value);
        size += Float.BYTES;
        return this;
    }

    public DynamicByteArray appendDouble(double value)
    {
        resizeIfNecessary(Double.BYTES);
        unsafe.putDouble(bytes, (long) ARRAY_BYTE_BASE_OFFSET + size, value);
        size += Double.BYTES;
        return this;
    }

    private void resizeIfNecessary(int length)
    {
        if (size + length >= bytes.length) {
            bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, size + length));
            underlyingSlice = Slices.wrappedBuffer(bytes);
        }
    }

    public byte getByte(int byteOffset)
    {
        if (byteOffset >= size) {
            throw new IllegalArgumentException();
        }
        return bytes[byteOffset];
    }

    public short getShort(int byteOffset)
    {
        if (byteOffset + Short.BYTES >= size || byteOffset < 0) {
            throw new IllegalArgumentException();
        }
        return unsafe.getShort(bytes, (long) ARRAY_BYTE_BASE_OFFSET + byteOffset);
    }

    public int getInt(int byteOffset)
    {
        if (byteOffset + Integer.BYTES >= size || byteOffset < 0) {
            throw new IllegalArgumentException();
        }
        return unsafe.getInt(bytes, (long) ARRAY_BYTE_BASE_OFFSET + byteOffset);
    }

    public long getLong(int byteOffset)
    {
        if (byteOffset + Long.BYTES >= size || byteOffset < 0) {
            throw new IllegalArgumentException();
        }
        return unsafe.getLong(bytes, (long) ARRAY_BYTE_BASE_OFFSET + byteOffset);
    }

    public float getFloat(int byteOffset)
    {
        if (byteOffset + Float.BYTES >= size || byteOffset < 0) {
            throw new IllegalArgumentException();
        }
        return unsafe.getFloat(bytes, (long) ARRAY_BYTE_BASE_OFFSET + byteOffset);
    }

    public double getDouble(int byteOffset)
    {
        if (byteOffset + Double.BYTES >= size || byteOffset < 0) {
            throw new IllegalArgumentException();
        }
        return unsafe.getDouble(bytes, (long) ARRAY_BYTE_BASE_OFFSET + byteOffset);
    }

    public Slice getSlice(int byteOffset, int length)
    {
        if (byteOffset + length >= size || byteOffset < 0) {
            throw new IllegalArgumentException();
        }
        return underlyingSlice.slice(byteOffset, length);
    }

    public int size()
    {
        return size;
    }

    public long getByteSize()
    {
        return SizeOf.sizeOf(bytes);
    }

    public long getRetainedByteSize()
    {
        return INSTANCE_SIZE + getByteSize();
    }

    public DynamicByteArray copyRegion(int position, int length)
    {
        if (position + length > size) {
            throw new IllegalArgumentException("Invalid region to copy");
        }
        return wrap(Arrays.copyOfRange(bytes, position, position + length));
    }

    public Slice getUnderlyingSlice()
    {
        return underlyingSlice;
    }

    public Slice slice()
    {
        return Slices.wrappedBuffer(bytes, 0, size);
    }

    @Override
    public String toString()
    {
        return "DynamicByteArray{" +
                "bytes=" + Arrays.toString(bytes) +
                ", size=" + size +
                '}';
    }
}
