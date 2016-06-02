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

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class BigintBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<BigintBlockEncoding> FACTORY = new BigintBlockEncodingFactory();
    public static final BigintBlockEncoding INSTANCE = new BigintBlockEncoding();
    private static final String NAME = "BIGINT";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // write null bits 8 at a time
        encodeNullsAsBits(sliceOutput, block);

        for (int i = 0; i < block.getPositionCount(); i++) {
            sliceOutput.appendLong(block.getLong(i, 0));
        }
    }

    @Override
    public int getEstimatedSize(Block block)
    {
        int positionCount = block.getPositionCount();

        int size = SIZE_OF_INT; // positionCount integer bytes
        size += positionCount / Byte.SIZE + 1; // one byte null bits per eight elements and possibly last null bits
        size += SIZE_OF_LONG * positionCount; // data bytes
        return size;
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        DynamicLongArray values = new DynamicLongArray(positionCount)
                .append(sliceInput, positionCount);

        return new BigintBlock(values, DynamicBooleanArray.wrap(valueIsNull));
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class BigintBlockEncodingFactory
            implements BlockEncodingFactory<BigintBlockEncoding>
    {
        private static final BigintBlockEncoding INSTANCE = new BigintBlockEncoding();

        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public BigintBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return INSTANCE;
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, BigintBlockEncoding blockEncoding)
        {
        }
    }
}
