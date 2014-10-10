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

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.UnloadedIndexKeyRecordSet.UnloadedIndexKeyRecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class IndexSnapshotBuilder
{
    private final int expectedPositions;
    private final List<Type> outputTypes;
    private final List<Type> missingKeysTypes;
    private final List<Integer> keyOutputChannels;
    private final List<Integer> missingKeysChannels;
    private final long maxMemoryInBytes;

    private final OperatorContext bogusOperatorContext;
    private PagesIndex outputPagesIndex;
    private PagesIndex missingKeysIndex;
    private LookupSource missingKeys;

    private final List<Page> pages = new ArrayList<>();
    private long memoryInBytes;

    public IndexSnapshotBuilder(List<Type> outputTypes,
            List<Integer> keyOutputChannels,
            DriverContext driverContext,
            DataSize maxMemoryInBytes,
            int expectedPositions)
    {
        checkNotNull(outputTypes, "outputTypes is null");
        checkNotNull(keyOutputChannels, "keyOutputChannels is null");
        checkNotNull(driverContext, "driverContext is null");
        checkNotNull(maxMemoryInBytes, "maxMemoryInBytes is null");
        checkArgument(expectedPositions > 0, "expectedPositions must be greater than zero");

        this.outputTypes = ImmutableList.copyOf(outputTypes);
        this.expectedPositions = expectedPositions;
        this.keyOutputChannels = ImmutableList.copyOf(keyOutputChannels);
        this.maxMemoryInBytes = maxMemoryInBytes.toBytes();

        ImmutableList.Builder<Type> missingKeysTypes = ImmutableList.builder();
        ImmutableList.Builder<Integer> missingKeysChannels = ImmutableList.builder();
        for (int i = 0; i < keyOutputChannels.size(); i++) {
            Integer keyOutputChannel = keyOutputChannels.get(i);
            missingKeysTypes.add(outputTypes.get(keyOutputChannel));
            missingKeysChannels.add(i);
        }
        this.missingKeysTypes = missingKeysTypes.build();
        this.missingKeysChannels = missingKeysChannels.build();

        // create a bogus operator context with unlimited memory for the pages index
        this.bogusOperatorContext = new TaskContext(driverContext.getTaskId(), driverContext.getExecutor(), driverContext.getSession(), new DataSize(Long.MAX_VALUE, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext()
                .addOperatorContext(0, "operator");

        this.outputPagesIndex = new PagesIndex(outputTypes, expectedPositions, bogusOperatorContext);
        this.missingKeysIndex = new PagesIndex(missingKeysTypes.build(), expectedPositions, bogusOperatorContext);
        this.missingKeys = missingKeysIndex.createLookupSource(this.missingKeysChannels);
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public long getMemoryInBytes()
    {
        return memoryInBytes;
    }

    public boolean isMemoryExceeded()
    {
        return memoryInBytes > maxMemoryInBytes;
    }

    public boolean tryAddPage(Page page)
    {
        memoryInBytes += page.getDataSize().toBytes();
        if (isMemoryExceeded()) {
            return false;
        }
        pages.add(page);
        return true;
    }

    public IndexSnapshot createIndexSnapshot(UnloadedIndexKeyRecordSet indexKeysRecordSet)
    {
        checkArgument(indexKeysRecordSet.getColumnTypes().equals(missingKeysTypes), "indexKeysRecordSet must have same schema as missingKeys");
        checkState(!isMemoryExceeded(), "Max memory exceeded");
        for (Page page : pages) {
            outputPagesIndex.addPage(page);
        }
        pages.clear();

        LookupSource lookupSource = outputPagesIndex.createLookupSource(keyOutputChannels);

        // Build a page containing the keys that produced no output rows, so in future requests can skip these keys
        PageBuilder missingKeysPageBuilder = new PageBuilder(missingKeysIndex.getTypes());
        UnloadedIndexKeyRecordCursor indexKeysRecordCursor = indexKeysRecordSet.cursor();
        while (indexKeysRecordCursor.advanceNextPosition()) {
            Block[] blocks = indexKeysRecordCursor.getBlocks();
            int position = indexKeysRecordCursor.getPosition();
            if (lookupSource.getJoinPosition(position, blocks) < 0) {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i].appendTo(position, missingKeysPageBuilder.getBlockBuilder(i));
                }
            }
        }
        Page missingKeysPage = missingKeysPageBuilder.build();

        memoryInBytes += missingKeysPage.getDataSize().toBytes();
        if (isMemoryExceeded()) {
            return null;
        }

        // only update missing keys if we have new missing keys
        if (!missingKeysPageBuilder.isEmpty()) {
            missingKeysIndex.addPage(missingKeysPage);
            missingKeys = missingKeysIndex.createLookupSource(missingKeysChannels);
        }

        return new IndexSnapshot(lookupSource, missingKeys);
    }

    public void reset()
    {
        memoryInBytes = 0;
        pages.clear();
        outputPagesIndex = new PagesIndex(outputTypes, expectedPositions, bogusOperatorContext);
        missingKeysIndex = new PagesIndex(missingKeysTypes, expectedPositions, bogusOperatorContext);
    }
}
