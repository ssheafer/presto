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
package com.facebook.presto.operator;

import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHash
{
    private static final int POSITIONS = 10_000_000;
    private static final String GROUP_COUNT_STRING = "500000";
    private static final int GROUP_COUNT = Integer.parseInt(GROUP_COUNT_STRING);
    private static final int EXPECTED_SIZE = 10_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object groupByHashPreCompute(BenchmarkData data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false);
        data.getPages().forEach(groupByHash::getGroupIds);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object baldrGroupByHashPreCompute(BenchmarkDataBaldr data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false);
        data.getPages().forEach(groupByHash::getGroupIds);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagePreCompute(BenchmarkData data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false);
        data.getPages().forEach(groupByHash::addPage);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object bigintGroupByHash(SingleChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new BigintGroupByHash(0, data.getHashEnabled(), EXPECTED_SIZE);
        data.getPages().forEach(groupByHash::addPage);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baseline(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        long[] table = new long[hashSize];
        Arrays.fill(table, -1);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = block.getLong(position, 0);

                int tablePosition = (int) (value & mask);
                while (table[tablePosition] != -1 && table[tablePosition] != value) {
                    tablePosition++;
                }
                if (table[tablePosition] == -1) {
                    table[tablePosition] = value;
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baselineBigArray(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        LongBigArray table = new LongBigArray(-1);
        table.ensureCapacity(hashSize);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = BIGINT.getLong(block, position);

                int tablePosition = (int) XxHash64.hash(value) & mask;
                while (table.get(tablePosition) != -1 && table.get(tablePosition) != value) {
                    tablePosition++;
                }
                if (table.get(tablePosition) == -1) {
                    table.set(tablePosition, value);
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    public static double root(double num, double root)
    {
        return Math.pow(Math.E, Math.log(num) / root);
    }

    private static List<Page> createPages(int positionCount, int groupCount, List<Type> types, boolean hashEnabled)
    {
        int channelCount = types.size();

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        int partialGroupCount = (int) Math.max(Math.round(root(groupCount, channelCount)), 1);
        System.out.println("PARTIAL:" + partialGroupCount);

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            long hash = 0;
            pageBuilder.declarePosition();
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                int rand = ThreadLocalRandom.current().nextInt(partialGroupCount);
                BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
                hash = CombineHashFunction.getHash(hash, rand);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), hash);
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BaselinePagesData
    {
        @Param("1")
        private int channelCount = 1;

        @Param("false")
        private boolean hashEnabled;

        @Param(GROUP_COUNT_STRING)
        private int groupCount;

        private List<Page> pages;

        @Setup
        public void setup()
        {
            pages = createPages(POSITIONS, groupCount, ImmutableList.of(BIGINT), hashEnabled);
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class SingleChannelBenchmarkData
    {
        @Param("1")
        private int channelCount = 1;

        @Param({"true", "false"})
        private boolean hashEnabled = true;

        @Param(GROUP_COUNT_STRING)
        private int groupCount = GROUP_COUNT;

        private List<Page> pages;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            pages = createPages(POSITIONS, GROUP_COUNT, ImmutableList.of(BIGINT), hashEnabled);
            types = Collections.<Type>nCopies(1, BIGINT);
            channels = new int[1];
            for (int i = 0; i < 1; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public boolean getHashEnabled()
        {
            return hashEnabled;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({ "1", "2", "3", "4"})
        private int channelCount = 1;

        // todo add more group counts when JMH support programmatic ability to set OperationsPerInvocation
        @Param({"250000", "500000", "1000000", "2000000", "4000000"})
        private int groupCount = GROUP_COUNT;

        @Param({"true", "false"})
        private boolean hashEnabled;

        private List<Page> pages;
        private Optional<Integer> hashChannel;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            pages = createPages(POSITIONS, groupCount, Collections.<Type>nCopies(channelCount, BIGINT), hashEnabled);
            hashChannel = hashEnabled ? Optional.of(channelCount) : Optional.empty();
            types = Collections.<Type>nCopies(channelCount, BIGINT);
            channels = new int[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Optional<Integer> getHashChannel()
        {
            return hashChannel;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkDataBaldr
    {
        private List<Page> pages;
        private int[] channels = new int[2];

        @Setup
        public void setup()
        {
            pages = createBaldrPages(POSITIONS);
            channels = new int[2];
            for (int i = 0; i < 2; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Optional<Integer> getHashChannel()
        {
            return Optional.of(2);
        }

        public List<Type> getTypes()
        {
            return ImmutableList.of(BIGINT, VARCHAR);
        }

        public int[] getChannels()
        {
            return channels;
        }
    }

    private static List<Page> createBaldrPages(int positionCount)
    {
        String[] arr = {
                "like",
                "comment",
                "friend",
                "post",
                "reshare",
                "photo",
                "feedback_reaction",
                "contact_field",
                "fan",
                "tag",
                "mention",
                "sticker",
                "og_post",
                "link",
                "installation",
                "group_user",
                "profile",
                "plan",
                "photo_album",
                "follow",
                "poke",
                "video",
                "phone_number",
                "email",
                "product_item",
                "page",
                "life_event",
                "streaming_feedback_reaction",
                "answer",
                "relationship",
                "profile_intro_card_bio_record",
                "family",
                "discussion_comment",
                "subscribe_calendar",
                "mark_safe",
                "groups",
                "admin",
                "file",
                "anniversary",
                "shared_album",
                "media_question_vote",
                "coupon",
                "note",
                "question",
                "group_doc",
                "checkin",
                "scrapbook",
                "marketplace_post",
                "fundraiser_donation",
        };

        ImmutableList<AbstractType> types = ImmutableList.of(BIGINT, VARCHAR);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            long hash = 0;
            pageBuilder.declarePosition();

            int rand = ThreadLocalRandom.current().nextInt(250_000);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), rand);
            hash = CombineHashFunction.getHash(hash, rand);

            Slice slice = Slices.utf8Slice(arr[ThreadLocalRandom.current().nextInt(arr.length)]);
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), slice);
            hash = CombineHashFunction.getHash(hash, XxHash64.hash(slice));

            BIGINT.writeLong(pageBuilder.getBlockBuilder(2), hash);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkGroupByHash().groupByHashPreCompute(data);
        new BenchmarkGroupByHash().addPagePreCompute(data);

        SingleChannelBenchmarkData singleChannelBenchmarkData = new SingleChannelBenchmarkData();
        singleChannelBenchmarkData.setup();
        new BenchmarkGroupByHash().bigintGroupByHash(singleChannelBenchmarkData);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGroupByHash.class.getSimpleName() + ".*roupByHashPreCompute")
                .param("hashEnabled", "true")
                .warmupIterations(1)
                .measurementIterations(1)
                .build();
        new Runner(options).run();
    }
}
