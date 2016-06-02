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

public class VariableWidthBlock
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlock.class).instanceSize();

    private final int positionOffset;
    private final int positionCount;
    private final DynamicByteArray bytes;
    private final DynamicIntArray offsets;
    private final DynamicBooleanArray valueIsNull;

    public VariableWidthBlock(int positionOffset, int positionCount, DynamicByteArray bytes, DynamicIntArray offsets, DynamicBooleanArray valueIsNull)
    {
        this.positionOffset = positionOffset;
        this.positionCount = positionCount;
        this.bytes = requireNonNull(bytes, "bytes is null");
        this.offsets = requireNonNull(offsets, "offsets is null");;
        this.valueIsNull = requireNonNull(valueIsNull, "valueIsNull is null");

        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset must be >= 0: " + positionOffset);
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount must be >= 0: " + positionCount);
        }
        if (offsets.size() < positionOffset + positionCount + 1) {
            throw new IllegalArgumentException("region wider than available offsets");
        }
        if (valueIsNull.size() < positionOffset + positionCount) {
            throw new IllegalArgumentException("region wider than available valueIsNull");
        }
        if (bytes.size() < offsets.get(positionOffset + positionCount)) {
            throw new IllegalArgumentException("region wider than available bytes");
        }
    }

    public VariableWidthBlock(DynamicByteArray bytes, DynamicIntArray offsets, DynamicBooleanArray valueIsNull)
    {
        this(0, valueIsNull.size(), bytes, offsets, valueIsNull);
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets.get(positionOffset + position);
    }

    @Override
    public int getLength(int position)
    {
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull.get(positionOffset + position);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
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
    public String toString()
    {
        return "VariableWidthBlock{" +
                "positionCount=" + positionCount +
                ", bytes=" + bytes +
                '}';
    }
}
