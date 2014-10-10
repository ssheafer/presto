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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.index.IndexSnapshot.UNLOADED_INDEX_KEY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class UnloadedIndexKeyRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final List<PageAndPositions> pageAndPositions;

    public UnloadedIndexKeyRecordSet(IndexSnapshot existingSnapshot, Set<Integer> channelsForDistinct, List<Type> types, List<UpdateRequest> requests)
    {
        checkNotNull(existingSnapshot, "existingSnapshot is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        checkNotNull(requests, "requests is null");

        int[] distinctChannels = Ints.toArray(channelsForDistinct);
        int[] normalizedDistinctChannels = new int[distinctChannels.length];
        List<Type> distinctChannelTypes = new ArrayList<>(distinctChannels.length);
        for (int i = 0; i < distinctChannels.length; i++) {
            normalizedDistinctChannels[i] = i;
            distinctChannelTypes.add(types.get(distinctChannels[i]));
        }

        ImmutableList.Builder<PageAndPositions> builder = ImmutableList.builder();
        long nextDistinctId = 0;
        GroupByHash groupByHash = new GroupByHash(distinctChannelTypes, normalizedDistinctChannels, 10_000);
        for (UpdateRequest request : requests) {
            IntList positions = new IntArrayList();
            Block[] blocks = request.getBlocks();

            Block[] distinctBlocks = new Block[distinctChannels.length];
            for (int i = 0; i < distinctBlocks.length; i++) {
                distinctBlocks[i] = blocks[distinctChannels[i]];
            }

            // Move through the positions while advancing the cursors in lockstep
            int positionCount = blocks[0].getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                // We are reading ahead in the cursors, so we need to filter any nulls since they can not join
                if (!containsNullValue(position, blocks)) {
                    // Only include the key if it is not already in the index
                    if (existingSnapshot.getJoinPosition(position, blocks) == UNLOADED_INDEX_KEY) {
                        // Only add the position if we have not seen this tuple before (based on the distinct channels)
                        if (groupByHash.putIfAbsent(position, distinctBlocks) == nextDistinctId) {
                            nextDistinctId++;
                            positions.add(position);
                        }
                    }
                }
            }

            if (!positions.isEmpty()) {
                builder.add(new PageAndPositions(request, positions));
            }
        }

        pageAndPositions = builder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public UnloadedIndexKeyRecordCursor cursor()
    {
        return new UnloadedIndexKeyRecordCursor(types, pageAndPositions);
    }

    private static boolean containsNullValue(int position, Block... blocks)
    {
        for (Block block : blocks) {
            if (block.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    public static class UnloadedIndexKeyRecordCursor
            implements RecordCursor
    {
        private final List<Type> types;
        private final Iterator<PageAndPositions> pageAndPositionsIterator;
        private Block[] blocks;
        private IntListIterator positionIterator;
        private int position;

        public UnloadedIndexKeyRecordCursor(List<Type> types, List<PageAndPositions> pageAndPositions)
        {
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.pageAndPositionsIterator = checkNotNull(pageAndPositions, "pageAndPositions is null").iterator();
            this.blocks = new Block[types.size()];
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (positionIterator == null || !positionIterator.hasNext()) {
                if (!pageAndPositionsIterator.hasNext()) {
                    return false;
                }
                PageAndPositions pageAndPositions = pageAndPositionsIterator.next();
                blocks = pageAndPositions.getUpdateRequest().getBlocks();
                checkState(types.size() == blocks.length);
                positionIterator = pageAndPositions.getPositions().iterator();
            }

            position = positionIterator.nextInt();

            return true;
        }

        public Block[] getBlocks()
        {
            return blocks;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return blocks[field].getBoolean(position);
        }

        @Override
        public long getLong(int field)
        {
            return blocks[field].getLong(position);
        }

        @Override
        public double getDouble(int field)
        {
            return blocks[field].getDouble(position);
        }

        @Override
        public Slice getSlice(int field)
        {
            return blocks[field].getSlice(position);
        }

        @Override
        public boolean isNull(int field)
        {
            return blocks[field].isNull(position);
        }

        @Override
        public void close()
        {
            // Do nothing
        }
    }

    private static class PageAndPositions
    {
        private final UpdateRequest updateRequest;
        private final IntList positions;

        private PageAndPositions(UpdateRequest updateRequest, IntList positions)
        {
            this.updateRequest = checkNotNull(updateRequest, "updateRequest is null");
            this.positions = checkNotNull(positions, "positions is null");
        }

        private UpdateRequest getUpdateRequest()
        {
            return updateRequest;
        }

        private IntList getPositions()
        {
            return positions;
        }
    }
}
