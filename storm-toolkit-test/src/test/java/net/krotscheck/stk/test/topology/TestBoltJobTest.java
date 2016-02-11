/*
 * Copyright (c) 2016 Michael Krotscheck
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.krotscheck.stk.test.topology;

import net.krotscheck.stk.component.AbstractBolt;
import net.krotscheck.stk.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.Testing;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * Tests that make sure the TestBoltJob does what it's supposed to.
 *
 * @author Michael Krotscheck
 */
public final class TestBoltJobTest {

    /**
     * Some common test data. Clone if you're going to use this.
     */
    private List<SortedMap<String, Object>> testData;

    /**
     * Create some testing classes.
     *
     * @throws Exception Should never be thrown.
     */
    @Before
    public void setUp() throws Exception {
        testData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TreeMap<String, Object> row = new TreeMap<>();
            row.put("col_1", i);
            row.put("col_2", String.valueOf(i));
            row.put("col_3", i % 2 == 1);
            testData.add(row);
        }
    }

    /**
     * Clean up.
     *
     * @throws Exception This should never be thrown.
     */
    @After
    public void tearDown() throws Exception {
        testData = null;
    }

    /**
     * Assert that we can create a test bolt job with a single stream of data.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testSingleStreamJob() throws Exception {
        TestBolt tb = new TestBolt();

        // Run the job.
        TestBoltJob job = new TestBoltJob(tb);
        job.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        Testing.withLocalCluster(job);

        Map<String, List<SortedMap<String, Object>>> allResults =
                job.getResults();
        Assert.assertEquals(1, allResults.size());
        Assert.assertTrue(allResults.containsKey(Utils.DEFAULT_STREAM_ID));

        List<SortedMap<String, Object>> results =
                job.getResults(Utils.DEFAULT_STREAM_ID);
        Assert.assertEquals(testData, results);
    }

    /**
     * Assert that we can create a test bolt job with a double stream of
     * unlinked data.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testDoubleStreamJob() throws Exception {
        TestBolt tb = new TestBolt();

        // Run the job.
        TestBoltJob job = new TestBoltJob(tb);
        job.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        job.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        Testing.withLocalCluster(job);

        Map<String, List<SortedMap<String, Object>>> allResults =
                job.getResults();
        Assert.assertEquals(1, allResults.size());
        Assert.assertTrue(allResults.containsKey(Utils.DEFAULT_STREAM_ID));

        List<SortedMap<String, Object>> results =
                job.getResults(Utils.DEFAULT_STREAM_ID);

        // Remove all tuples that exist in our expectd data, as they might
        // not be sorted.
        List<SortedMap<String, Object>> combinedTestData = new ArrayList<>();
        combinedTestData.addAll(testData);
        combinedTestData.addAll(new ArrayList<>(testData));
        List<SortedMap<String, Object>> result =
                job.getResults(Utils.DEFAULT_STREAM_ID);

        for (SortedMap<String, Object> tuple : combinedTestData) {
            Assert.assertTrue(result.contains(tuple));
        }
    }

    /**
     * Assert that we can create a test bolt job with a streams of different
     * names.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testDoubleDiffNameJob() throws Exception {
        String otherStreamName = "otherStreamName";
        TestBolt tb = new TestBolt();

        // Run the job.
        TestBoltJob job = new TestBoltJob(tb);
        job.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        job.addInputData(otherStreamName, testData);
        Testing.withLocalCluster(job);

        Map<String, List<SortedMap<String, Object>>> allResults =
                job.getResults();
        Assert.assertEquals(2, allResults.size());
        Assert.assertTrue(allResults.containsKey(Utils.DEFAULT_STREAM_ID));
        Assert.assertTrue(allResults.containsKey(otherStreamName));

        Assert.assertEquals(testData, job.getResults(Utils.DEFAULT_STREAM_ID));
        Assert.assertEquals(testData, job.getResults(otherStreamName));
    }

    /**
     * A bolt useful for our testing.
     */
    private static final class TestBolt extends AbstractBolt {

        /**
         * Update this bolt's configuration based on the storm configuration and
         * the topology context.
         *
         * @param stormConf The Storm configuration for this bolt. This is the
         *                  configuration provided to the topology merged in
         *                  with cluster configuration on this machine.
         * @param context   This object can be used to get information about
         *                  this task's place within the topology, including the
         *                  task id and component id of this task, input and
         *                  output
         */
        @Override
        protected void configure(final Map stormConf,
                                 final TopologyContext context) {
            // Do nothing.
        }

        /**
         * Whenever the provided streams are changed, this method is invoked to
         * trigger the component to recalculate the emitted streams.
         *
         * @param providedStreams The number of streams provided to this
         *                        component.
         * @return A list of emitted streams.
         */
        @Override
        protected Collection<Stream> calculateEmittedStreams(
                final Collection<Stream> providedStreams) {
            return Collections.unmodifiableCollection(providedStreams);
        }

        /**
         * Process a single tuple of input. The Tuple object contains metadata
         * on it about which component/stream/task it came from. The values of
         * the Tuple can be accessed using Tuple#getValue. The IBolt does not
         * have to process the Tuple immediately. It is perfectly fine to hang
         * onto a tuple and process it later (for instance, to do an aggregation
         * or join).
         *
         * <p>Tuples should be emitted using the OutputCollector provided
         * through the prepare method. It is required that all input tuples are
         * acked or failed at some point using the OutputCollector. Otherwise,
         * Storm will be unable to determine when tuples coming off the spouts
         * have been completed.</p>
         *
         * <p>For the common case of acking an input tuple at the end of the
         * execute method, see IBasicBolt which automates this.</p>
         *
         * @param input The input tuple to be processed.
         */
        @Override
        public void execute(final Tuple input) {
            SortedMap<String, Object> output = new TreeMap<>();
            for (String field : input.getFields()) {
                output.put(field, input.getValueByField(field));
            }

            emit(input.getSourceStreamId(), input, output);
            ack(input);
        }

        /**
         * Declare configuration specific to this component.
         */
        @Override
        public Map<String, Object> getComponentConfiguration() {
            // Do nothing.
            return new HashMap<>();
        }
    }
}
