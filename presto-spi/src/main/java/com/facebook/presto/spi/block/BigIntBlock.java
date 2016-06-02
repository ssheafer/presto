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

import static java.util.Objects.requireNonNull;

public class BigintBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintBlock.class).instanceSize();

    private final DynamicLongArray values;
    private final DynamicBooleanArray valueIsNull;
    private final int offset;
    private final int positionCount;

    public BigintBlock(DynamicLongArray values, DynamicBooleanArray valueIsNull)
    {
        this(0, values.size(), values, valueIsNull);
    }

    public BigintBlock(int offset, int positionCount, DynamicLongArray values, DynamicBooleanArray valueIsNull)
    {
        // TODO: immutable arrays?
        this.values = requireNonNull(values, "values is null");
        this.valueIsNull = requireNonNull(valueIsNull, "valueIsNull is null");
        this.offset = offset;
        this.positionCount = positionCount;
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0: " + offset);
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount must be >= 0: " + positionCount);
        }
        if (offset + positionCount > values.size()) {
            throw new IllegalArgumentException("region wider than available values");
        }
        if (offset + positionCount > valueIsNull.size()) {
            throw new IllegalArgumentException("region wider than available valueIsNull");
        }
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
                DynamicLongArray.wrap(getLong(position)),
                DynamicBooleanArray.wrap(isNull(position)));
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
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
        return new BigintBlock(offset + positionOffset, length, values, valueIsNull);
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
        checkPosition(position); // TODO: ?
        return valueIsNull.get(offset + position);
    }

    private long getLong(int position)
    {
        checkPosition(position); // TODO: ?
        return values.get(offset + position);
    }

    private void checkPosition(int position)
    {
        if (position >= positionCount) {
            throw new IllegalArgumentException("Position greater than position count: " + position);
        }
    }

    @Override
    public void assureLoaded()
    {
    }

    @Override
    public String toString()
    {
        return "BigintBlock{" +
                "values=" + values +
                ", valueIsNull=" + valueIsNull +
                '}';
    }
}
