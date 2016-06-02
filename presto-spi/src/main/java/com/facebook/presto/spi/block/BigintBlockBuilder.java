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

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

public class BigintBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintBlockBuilder.class).instanceSize() + BlockBuilderStatus.INSTANCE_SIZE;

    private final BlockBuilderStatus blockBuilderStatus;
    private final DynamicLongArray values;
    private final DynamicBooleanArray valueIsNull;

    public BigintBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.values = new DynamicLongArray(expectedEntries);
        this.valueIsNull = new DynamicBooleanArray(expectedEntries);
    }

    @Override
    public int getLength(int position)
    {
        return Long.BYTES;
    }

    @Override
    public byte getByte(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int position, int offset)
    {
        return getLong(position);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        blockBuilder.writeLong(getLong(position));
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return new BigintBlock(
                new DynamicLongArray(1).append(getLong(position)),
                new DynamicBooleanArray(1).append(isNull(position)));
    }

    @Override
    public int getPositionCount()
    {
        return values.size();
    }

    @Override
    public int getSizeInBytes()
    {
        long byteSize = values.getByteSize() + valueIsNull.getByteSize();
        return byteSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) byteSize;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        long byteSize = INSTANCE_SIZE + values.getRetainedByteSize() + valueIsNull.getRetainedByteSize();
        return byteSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) byteSize;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return BigintBlockEncoding.INSTANCE;
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        DynamicLongArray newValues = new DynamicLongArray(positions.size());
        DynamicBooleanArray newValueIsNull = new DynamicBooleanArray(positions.size());

        for (int position : positions) {
            newValues.append(getLong(position));
            newValueIsNull.append(isNull(position));
        }
        return new BigintBlock(newValues, newValueIsNull);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return new BigintBlock(positionOffset, length, values, valueIsNull);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        return new BigintBlock(
                values.copyRegion(position, length),
                valueIsNull.copyRegion(position, length));
    }

    @Override
    public boolean isNull(int position)
    {
        return valueIsNull.get(position);
    }

    private long getLong(int position)
    {
        return values.get(position);
    }

    @Override
    public void assureLoaded()
    {
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        values.append(value);
        valueIsNull.append(false);
        blockBuilderStatus.addBytes(Byte.BYTES + Long.BYTES);
        return this;
    }

    @Override
    public BlockBuilder writeFloat(float value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeDouble(double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder closeEntry()
    {
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        // fixed width is always written regardless of null flag
        values.append(0);
        valueIsNull.append(true);
        blockBuilderStatus.addBytes(Byte.BYTES + Long.BYTES);
        return this;
    }

    @Override
    public Block build()
    {
        return new BigintBlock(values, valueIsNull);
    }

    @Override
    public String toString()
    {
        return "BigintBlockBuilder{" +
                "values=" + values +
                ", valueIsNull=" + valueIsNull +
                '}';
    }
}
