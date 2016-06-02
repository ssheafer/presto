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
import java.util.Objects;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockBuilder.class).instanceSize() + BlockBuilderStatus.INSTANCE_SIZE;

    private final BlockBuilderStatus blockBuilderStatus;
    private final DynamicByteArray bytes;
    private final DynamicIntArray offsets;
    private final DynamicBooleanArray valueIsNull;

    private int currentEntrySize;

    public VariableWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this.blockBuilderStatus = Objects.requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.bytes = new DynamicByteArray(expectedBytesPerEntry * expectedEntries);
        this.offsets = new DynamicIntArray(expectedEntries + 1)
                .append(0);
        this.valueIsNull = new DynamicBooleanArray(expectedEntries);
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets.get(position);
    }

    @Override
    public int getLength(int position)
    {
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull.get(position);
    }

    @Override
    public int getPositionCount()
    {
        return valueIsNull.size();
    }

    @Override
    public int getSizeInBytes()
    {
        long byteSize = bytes.getByteSize() + offsets.getByteSize() + valueIsNull.getByteSize();
        return byteSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) byteSize;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        long byteSize = INSTANCE_SIZE + bytes.getRetainedByteSize() + offsets.getRetainedByteSize() + valueIsNull.getRetainedByteSize();
        return byteSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) byteSize;
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        int finalLength = positions.stream().mapToInt(this::getLength).sum();
        DynamicByteArray newBytes = new DynamicByteArray(finalLength);
        DynamicIntArray newOffsets = new DynamicIntArray(positions.size() + 1);
        DynamicBooleanArray newValueIsNull = new DynamicBooleanArray(positions.size());

        newOffsets.append(0);
        for (int position : positions) {
            boolean isNull = isEntryNull(position);
            newValueIsNull.append(isNull);
            if (!isNull) {
                newBytes.appendBytes(bytes, getPositionOffset(position), getLength(position));
            }
            newOffsets.append(newBytes.size());
        }
        return new VariableWidthBlock(newBytes, newOffsets, newValueIsNull);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        // TODO: what position?
        return bytes.getUnderlyingSlice();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return new VariableWidthBlock(positionOffset, length, bytes, offsets, valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        DynamicIntArray newOffsets = new DynamicIntArray(length + 1)
                .append(0);

        int sliceLength = 0;
        for (int position = positionOffset; position < positionOffset + length; position++) {
            sliceLength += getLength(position);
            newOffsets.append(sliceLength);
        }

        DynamicByteArray newBytes = bytes.copyRegion(getPositionOffset(positionOffset), sliceLength);
        DynamicBooleanArray newValueIsNull = valueIsNull.copyRegion(positionOffset, length);
        return new VariableWidthBlock(newBytes, newOffsets, newValueIsNull);
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        bytes.appendByte((byte) value);
        currentEntrySize += SIZE_OF_BYTE;
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        bytes.appendShort((short) value);
        currentEntrySize += SIZE_OF_SHORT;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        bytes.appendInt(value);
        currentEntrySize += SIZE_OF_INT;
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        bytes.appendLong(value);
        currentEntrySize += SIZE_OF_LONG;
        return this;
    }

    @Override
    public BlockBuilder writeFloat(float value)
    {
        bytes.appendFloat(value);
        currentEntrySize += SIZE_OF_FLOAT;
        return this;
    }

    @Override
    public BlockBuilder writeDouble(double value)
    {
        bytes.appendDouble(value);
        currentEntrySize += SIZE_OF_DOUBLE;
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        bytes.appendBytes(source, sourceIndex, length);
        currentEntrySize += length;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        entryAdded(currentEntrySize, false);
        currentEntrySize = 0;
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(0, true);
        return this;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        valueIsNull.append(isNull);
        offsets.append(bytes.size());
        blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new VariableWidthBlock(bytes, offsets, valueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("size=").append(bytes.size());
        sb.append('}');
        return sb.toString();
    }
}
