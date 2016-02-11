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

import net.krotscheck.stk.stream.Schema;
import net.krotscheck.stk.stream.Stream;
import net.krotscheck.stk.stream.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MockedSources;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for the dummy data provider spout.
 *
 * @author Michael Krotscheck
 */
public final class TestSpoutTest {

    /**
     * Test storm configuration. Not ever modified, so final.
     */
    private final Map<String, Object> testStormConf = new HashMap<>();

    /**
     * Topology context for testing. Not ever used, so final.
     */
    private final TopologyContext tc = mock(TopologyContext.class);

    /**
     * Some common test data. Clone if you're going to use this.
     */
    private SortedMap<String, Object> testData;

    /**
     * Output collector for the spout under test.
     */
    private SpoutOutputCollector sc;

    /**
     * Create some testing classes.
     *
     * @throws Exception Should never be thrown.
     */
    @Before
    public void setUp() throws Exception {
        sc = mock(SpoutOutputCollector.class);
        testData = new TreeMap<>();
        testData.put("col_1", "bar");
        testData.put("col_2", true);
        testData.put("col_3", 3);
    }

    /**
     * Clean up.
     *
     * @throws Exception This should never be thrown.
     */
    @After
    public void tearDown() throws Exception {
        sc = null;
        testData = null;
    }

    /**
     * Assert that any provided ID is accessible.
     *
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testGetId() throws Exception {
        Integer id = 1;
        TestSpout t = new TestSpout(id);
        Assert.assertEquals(id, t.getId());
    }

    /**
     * Assert that you can add input data.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testAddInputData() throws Exception {
        TestSpout t = new TestSpout(1);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        List<SortedMap<String, Object>> placedData =
                t.getInputData(Utils.DEFAULT_STREAM_ID);

        List<SortedMap<String, Object>> testResult =
                new ArrayList<>();
        testResult.add(testData);

        Assert.assertEquals(testResult, placedData);
    }

    /**
     * Assert that input data of varying schemae are normalized to a merged
     * schema.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testAddInputDataNormalizeSchema() throws Exception {
        TestSpout t = new TestSpout(1);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        SortedMap<String, Object> otherData = new TreeMap<>(testData);
        otherData.put("col_x", "the_x_factor");
        t.addInputData(Utils.DEFAULT_STREAM_ID, otherData);

        List<SortedMap<String, Object>> placedData =
                t.getInputData(Utils.DEFAULT_STREAM_ID);

        Assert.assertEquals(2, placedData.size());
        SortedMap<String, Object> row = placedData.get(0);
        Assert.assertTrue(row.containsKey("col_x"));
    }

    /**
     * Assert that multiple additions of input data are concatenated into one
     * stream.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testAddInputDataConcat() throws Exception {
        TestSpout t = new TestSpout(1);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);

        List<SortedMap<String, Object>> placedData =
                t.getInputData(Utils.DEFAULT_STREAM_ID);

        Assert.assertEquals(3, placedData.size());
    }

    /**
     * Assert that you can batch add input data.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testAddInputDataBatch() throws Exception {
        TestSpout t = new TestSpout(1);
        List<SortedMap<String, Object>> batchData = new ArrayList<>();
        batchData.add(testData);
        batchData.add(testData);
        batchData.add(testData);
        t.addInputData(Utils.DEFAULT_STREAM_ID, batchData);

        List<SortedMap<String, Object>> placedData =
                t.getInputData(Utils.DEFAULT_STREAM_ID);

        Assert.assertEquals(3, placedData.size());
    }

    /**
     * Assert that you can add input data to more than one stream.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testAddInputDataMultiStream() throws Exception {
        TestSpout t = new TestSpout(1);
        List<SortedMap<String, Object>> batchData = new ArrayList<>();
        batchData.add(testData);
        batchData.add(testData);
        batchData.add(testData);
        t.addInputData("foo_bar", batchData);

        List<SortedMap<String, Object>> placedData =
                t.getInputData("foo_bar");

        Assert.assertEquals(3, placedData.size());
    }

    /**
     * This should do nothing.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testGetComponentConfiguration() throws Exception {
        TestSpout t = new TestSpout(1);
        Map<String, Object> config = t.getComponentConfiguration();
        Assert.assertNotNull(config);
        Assert.assertEquals(0, config.size());
    }

    /**
     * Assert that activation resets everything.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testActivate() throws Exception {
        TestSpout t = new TestSpout(1);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        t.open(testStormConf, tc, sc);

        t.nextTuple();
        verifyNoMoreInteractions(sc);
        Assert.assertEquals(0, t.getMessageIds().size());
        Assert.assertEquals(0, t.getSuccessMessageIds().size());
        Assert.assertEquals(0, t.getFailedMessageIds().size());

        t.activate();
        Assert.assertEquals(0, t.getMessageIds().size());
        Assert.assertEquals(0, t.getSuccessMessageIds().size());
        Assert.assertEquals(0, t.getFailedMessageIds().size());
        t.nextTuple();
        verify(sc).emit(
                Matchers.same(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(new ArrayList(testData.values())),
                Matchers.anyString());
        Assert.assertEquals(1, t.getMessageIds().size());
        Assert.assertEquals(0, t.getSuccessMessageIds().size());
        Assert.assertEquals(0, t.getFailedMessageIds().size());

        t.activate();
        Assert.assertEquals(0, t.getMessageIds().size());
        Assert.assertEquals(0, t.getSuccessMessageIds().size());
        Assert.assertEquals(0, t.getFailedMessageIds().size());
    }

    /**
     * Assert that deactivating clears everything.
     *
     * @throws Exception Should not be thrown
     */
    @Test
    public void testDeactivate() throws Exception {
        // Setup a messy tuple.
        TestSpout t = new TestSpout(1);
        t.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        t.open(testStormConf, tc, sc);
        t.activate();
        t.nextTuple();
        verify(sc).emit(
                Matchers.same(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(new ArrayList(testData.values())),
                Matchers.anyString());
        Assert.assertEquals(1, t.getMessageIds().size());
        Assert.assertEquals(0, t.getSuccessMessageIds().size());
        Assert.assertEquals(0, t.getFailedMessageIds().size());

        t.deactivate();
        Assert.assertEquals(0, t.getMessageIds().size());
        Assert.assertEquals(0, t.getSuccessMessageIds().size());
        Assert.assertEquals(0, t.getFailedMessageIds().size());
        t.nextTuple();
        verifyNoMoreInteractions(sc);
    }

    /**
     * Assert that nextTuple returns a provided tuple.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testNextTupleNormal() throws Exception {
        TestSpout s = new TestSpout(1);
        SortedMap<String, Object> td = new TreeMap<>(testData);
        s.addInputData(Utils.DEFAULT_STREAM_ID, td);
        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple();

        verify(sc).emit(
                Matchers.same(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(new ArrayList(td.values())),
                Matchers.anyString());
    }

    /**
     * Assert that nextTuple will move to the next iterator if the first is
     * empty.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testNextTupleGotoNext() throws Exception {
        TestSpout s = new TestSpout(1);

        SortedMap<String, Object> td1 = new TreeMap<>(testData);
        s.addInputData(Utils.DEFAULT_STREAM_ID, td1);

        SortedMap<String, Object> td2 = new TreeMap<>(testData);
        s.addInputData("other_stream_id", td2);

        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple();
        verify(sc).emit(
                Matchers.same(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(new ArrayList(td1.values())),
                Matchers.anyString());

        s.nextTuple();

        verify(sc).emit(
                Matchers.same("other_stream_id"),
                Matchers.eq(new ArrayList(td2.values())),
                Matchers.anyString());
    }

    /**
     * Assert that mismatched tuple schema are normalized.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testNextTupleNormalizedColumns() throws Exception {
        TestSpout s = new TestSpout(1);

        SortedMap<String, Object> td1 = new TreeMap<>(testData);
        s.addInputData(Utils.DEFAULT_STREAM_ID, td1);

        SortedMap<String, Object> td2 = new TreeMap<>(testData);
        td2.put("col_4", "bar");
        s.addInputData(Utils.DEFAULT_STREAM_ID, td2);

        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple();

        SortedMap<String, Object> expectedData = new TreeMap<>(td1);
        expectedData.put("col_4", null);

        verify(sc).emit(
                Matchers.same(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(new ArrayList(expectedData.values())),
                Matchers.anyString());
    }

    /**
     * Assert that nextTuple returns null when there are no iterators.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testNextTupleEmpty() throws Exception {
        TestSpout s = new TestSpout(1);

        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple(); // Drain the first one
        verifyNoMoreInteractions(sc);
    }

    /**
     * Assert that nextTuple returns null when all iterators are drained.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testNextTupleAllDone() throws Exception {
        TestSpout s = new TestSpout(1);

        SortedMap<String, Object> td1 = new TreeMap<>(testData);
        s.addInputData(Utils.DEFAULT_STREAM_ID, td1);

        SortedMap<String, Object> td2 = new TreeMap<>(testData);
        s.addInputData("other_stream_id", td2);

        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple(); // Drain the first one
        verify(sc).emit(
                Matchers.same(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(new ArrayList(td1.values())),
                Matchers.anyString());
        s.nextTuple(); // Drain the second one
        verify(sc).emit(
                Matchers.same("other_stream_id"),
                Matchers.eq(new ArrayList(td2.values())),
                Matchers.anyString());

        s.nextTuple();
        verifyNoMoreInteractions(sc);
    }

    /**
     * Assert that ack() moves a messageID from pending to success.
     *
     * @throws Exception Should not be thrown
     */
    @Test
    public void testAck() throws Exception {
        TestSpout s = new TestSpout(1);
        s.addInputData(Utils.DEFAULT_STREAM_ID, new TreeMap<>(testData));

        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple(); // Drain the first one

        List<String> pendingIds = s.getMessageIds();
        Assert.assertEquals(1, pendingIds.size());
        String firstId = pendingIds.get(0);

        s.ack(firstId);

        List<String> newIds = s.getMessageIds();
        List<String> successIds = s.getSuccessMessageIds();

        Assert.assertEquals(0, newIds.size());
        Assert.assertEquals(1, successIds.size());
        Assert.assertTrue(successIds.contains(firstId));
    }

    /**
     * Assert that failing a message moves it into failedMessageId's.
     *
     * @throws Exception Should not be thrown
     */
    @Test
    public void testFail() throws Exception {
        TestSpout s = new TestSpout(1);
        s.addInputData(Utils.DEFAULT_STREAM_ID, new TreeMap<>(testData));

        s.open(testStormConf, tc, sc);
        s.activate();
        s.nextTuple(); // Drain the first one

        List<String> pendingIds = s.getMessageIds();
        Assert.assertEquals(1, pendingIds.size());
        String firstId = pendingIds.get(0);

        s.fail(firstId);

        List<String> newIds = s.getMessageIds();
        List<String> failIds = s.getFailedMessageIds();

        Assert.assertEquals(0, newIds.size());
        Assert.assertEquals(1, failIds.size());
        Assert.assertTrue(failIds.contains(firstId));
    }

    /**
     * Assert that the spout returns appropriate value collections for its input
     * data.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testApply() throws Exception {
        TestSpout s = new TestSpout(1);

        // Test with nothing
        MockedSources mEmpty = mock(MockedSources.class);
        s.apply(mEmpty);

        verifyNoMoreInteractions(mEmpty);

        // Add some simple data.
        s.addInputData(Utils.DEFAULT_STREAM_ID, testData);
        Values testResults = new Values();
        for (String key : testData.keySet()) {
            testResults.add(testData.get(key));
        }
        MockedSources mSingle = mock(MockedSources.class);
        s.apply(mSingle);

        verify(mSingle).addMockData("1",
                Utils.DEFAULT_STREAM_ID,
                testResults);
    }

    /**
     * Assert that configure() does nothing.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testConfigure() throws Exception {
        TestSpout s = new TestSpout(1);
        s.open(testStormConf, tc, sc);
        s.configure(testStormConf, tc);
        verifyNoMoreInteractions(sc);
    }

    /**
     * Assert that emitted stream schemae are detected off of the input data.
     *
     * @throws Exception Should not be thrown.
     */
    @Test
    public void testCalculateEmittedStreams() throws Exception {
        TestSpout s = new TestSpout(1);
        Collection<Stream> emitted = s.calculateEmittedStreams();
        Assert.assertNotNull(emitted);
        Assert.assertEquals(0, emitted.size());

        // Add some data.
        s.addInputData(Utils.DEFAULT_STREAM_ID, testData);

        // Calculate the stream
        ArrayList<Stream> emitted2 =
                new ArrayList<>(s.calculateEmittedStreams());
        Assert.assertEquals(1, emitted2.size());

        // Make sure the default stream matches what we expect.
        Stream defaultStream = emitted2.get(0);
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID,
                defaultStream.getStreamId());
        Schema defaultSchema = defaultStream.getSchema();
        Assert.assertEquals(3, defaultSchema.size());

        Assert.assertEquals(Type.STRING, defaultSchema.get("col_1"));
        Assert.assertEquals(Type.BOOLEAN, defaultSchema.get("col_2"));
        Assert.assertEquals(Type.INT, defaultSchema.get("col_3"));
    }
}
