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

package net.krotscheck.stk.component;

import net.krotscheck.stk.stream.Stream;
import net.krotscheck.stk.stream.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test suite for the AbstractBolt Base Class.
 *
 * @author Michael Krotscheck
 */
public final class AbstractBoltTest {

    /**
     * Private stream name.
     */
    private final String testStreamName = "test_stream_name";

    /**
     * A single input anchor.
     */
    private Tuple anchor;

    /**
     * List of anchor tuples.
     */
    private Collection<Tuple> anchors;

    /**
     * A mapped output tuple.
     */
    private SortedMap<String, Object> outputTupleMap;

    /**
     * The above mapped output tuple's sorted values.
     */
    private List<Object> outputTuple;

    /**
     * A mapped output tuple.
     */
    private List<Integer> emittedTupleIds;

    /**
     * A test output collector.
     */
    private OutputCollector oc;

    /**
     * The provided stream.
     */
    private List<Stream> providedStreams;

    /**
     * Create some test data.
     */
    @Before
    public void setup() {
        anchor = mock(Tuple.class);
        anchors = new LinkedList<>();

        outputTupleMap = new TreeMap<>();
        outputTupleMap.put("col_0", false);
        outputTupleMap.put("col_1", 1);
        outputTupleMap.put("col_2", "two");

        outputTuple = new LinkedList<>(outputTupleMap.values());

        emittedTupleIds = new LinkedList<>();
        emittedTupleIds.add(1);

        oc = mock(OutputCollector.class);
        when(oc.emit(Matchers.eq(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(anchor),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);
        when(oc.emit(Matchers.eq(Utils.DEFAULT_STREAM_ID),
                Matchers.eq(anchors),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);

        when(oc.emit(Matchers.eq(testStreamName),
                Matchers.eq(anchor),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);
        when(oc.emit(Matchers.eq(testStreamName),
                Matchers.eq(anchors),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);

        providedStreams = new LinkedList<>();
        providedStreams.add(new Stream.Builder(Utils.DEFAULT_STREAM_ID)
                .addSchemaField("col_0", Type.BOOLEAN)
                .addSchemaField("col_1", Type.INT)
                .addSchemaField("col_2", Type.STRING)
                .build());
        providedStreams.add(new Stream.Builder(testStreamName)
                .addSchemaField("col_0", Type.BOOLEAN)
                .addSchemaField("col_1", Type.INT)
                .addSchemaField("col_2", Type.STRING)
                .build());
    }

    /**
     * Tear down the test data.
     */
    @After
    public void teardown() {
        anchor = null;
        anchors = null;
        outputTupleMap = null;
        outputTuple = null;
        emittedTupleIds = null;
        oc = null;
        providedStreams = null;
    }

    /**
     * Assert that the abstract bolt is abstract.
     *
     * @throws Exception Unexpected Exceptions.
     */
    @Test(expected = InstantiationException.class)
    public void testAbstractClass() throws Exception {
        net.krotscheck.stk.component.AbstractBolt.class.newInstance();
    }

    /**
     * Assert that the cleanup() method is implemented and final. Since this
     * method is not guaranteed to be invoked, we remove it from our API.
     */
    @Test
    public void testCleanupImplemented() {
        for (Method f : net.krotscheck.stk.component.AbstractBolt.class
                .getDeclaredMethods()) {
            if (f.getName().equals("cleanup")) {
                Assert.assertTrue(Modifier.isFinal(f.getModifiers()));
                return;
            }
        }
        Assert.assertFalse("method cleanup() is not defined", true);
    }

    /**
     * Assert that prepare calls invalidate the lifecycle.
     */
    @Test
    public void testLifecyclePrepare() {
        TestBolt b = spy(new TestBolt());

        // Assert that prepare calls invalidate
        OutputCollector oc = mock(OutputCollector.class);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);
        verify(b, times(1)).invalidate();
    }

    /**
     * Assert that invalidate calls configure() and invalidateEmittedStreams().
     */
    @Test
    public void testLifecycleInvalidate() {
        TestBolt b = spy(new TestBolt());

        // Assert that prepare calls invalidate
        b.invalidate();
        verify(b, times(1)).configure(null, null);
        verify(b, times(1)).calculateEmittedStreams(anyCollection());
    }

    /**
     * Assert that the lifecycle is properly invoked.
     */
    @Test
    public void testCleanupInvalidate() {
        TestBolt b = spy(new TestBolt());

        // Assert that prepare calls invalidate
        b.cleanup();
        verify(b, times(1)).invalidate();
    }

    /**
     * Test emit with the default stream and multiple anchors.
     */
    @Test
    public void testEmitDefaultStreamWithAnchors() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);

        // Send a tuple with no outputcollector
        List<Integer> results = b.emit(anchors, outputTupleMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        // Add an outputcollector
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        List<Integer> resultsOc = b.emit(anchors, outputTupleMap);
        Assert.assertNotNull(resultsOc);
        Assert.assertEquals(1, resultsOc.size());
        verify(oc, times(1))
                .emit(Utils.DEFAULT_STREAM_ID, anchors, outputTuple);
    }

    /**
     * Test emit with the default stream and one anchor.
     */
    @Test
    public void testEmitDefaultStreamWithAnchor() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);

        // Send a tuple with no outputcollector
        List<Integer> results = b.emit(anchor, outputTupleMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        // Add an outputcollector
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        List<Integer> resultsOc = b.emit(anchor, outputTupleMap);
        Assert.assertNotNull(resultsOc);
        Assert.assertEquals(1, resultsOc.size());
        verify(oc, times(1)).emit(Utils.DEFAULT_STREAM_ID, anchor, outputTuple);
    }

    /**
     * Test emit with a custom stream and multiple anchors.
     */
    @Test
    public void testEmitCustomStreamWithAnchors() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);

        // Send a tuple with no outputcollector
        List<Integer> results = b.emit(testStreamName, anchors, outputTupleMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        // Add an outputcollector
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        List<Integer> resultsOc =
                b.emit(testStreamName, anchors, outputTupleMap);
        Assert.assertNotNull(resultsOc);
        Assert.assertEquals(1, resultsOc.size());
        verify(oc, times(1)).emit(testStreamName, anchors, outputTuple);

    }

    /**
     * Test emit with a custom stream and multiple anchors.
     */
    @Test
    public void testEmitInvalidStreamWithAnchors() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);

        // Send a tuple without an outputCollector
        List<Integer> resultNoOc =
                b.emit("bad_stream_name", anchors, outputTupleMap);
        Assert.assertNotNull(resultNoOc);
        Assert.assertEquals(0, resultNoOc.size());

        // Send a tuple with an outputcollector.
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);
        List<Integer> results =
                b.emit("bad_stream_name", anchors, outputTupleMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        verifyNoMoreInteractions(oc);
    }

    /**
     * Test emit with a custom stream and one anchor.
     */
    @Test
    public void testEmitCustomStreamWithAnchor() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);

        // Send a tuple with no outputcollector
        List<Integer> results = b.emit(testStreamName, anchor, outputTupleMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        // Add an outputcollector
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        List<Integer> resultsOc =
                b.emit(testStreamName, anchor, outputTupleMap);
        Assert.assertNotNull(resultsOc);
        Assert.assertEquals(1, resultsOc.size());
        verify(oc, times(1)).emit(testStreamName, anchor, outputTuple);
    }

    /**
     * Test emit with an invalid stream name.
     */
    @Test
    public void testEmitInvalidStreamWithAnchor() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);

        // Send a tuple without an outputCollector
        List<Integer> resultNoOc =
                b.emit("bad_stream_name", anchor, outputTupleMap);
        Assert.assertNotNull(resultNoOc);
        Assert.assertEquals(0, resultNoOc.size());

        // Send a tuple with an outputcollector.
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);
        List<Integer> results =
                b.emit("bad_stream_name", anchor, outputTupleMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        verifyNoMoreInteractions(oc);
    }

    /**
     * Test emit with an incomplete tuple.
     */
    @Test
    public void testEmitIncompleteTuple() {
        TestBolt b = spy(new TestBolt());
        b.addProvidedStream(providedStreams);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        Map<String, Object> testMap = new TreeMap<>(outputTupleMap);
        testMap.remove("col_1");
        Map<String, Object> emittedMap = new TreeMap<>(testMap);
        emittedMap.put("col_1", null);
        List<Object> emittedTuple = new LinkedList<>(emittedMap.values());

        // Send a tuple with no outputcollector
        List<Integer> results =
                b.emit(testStreamName, anchor, testMap);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        verify(oc, times(1)).emit(testStreamName, anchor, emittedTuple);
    }

    /**
     * Test a single ack.
     */
    @Test
    public void testSingleAck() {
        // Various data mocks
        TestBolt b = spy(new TestBolt());
        Tuple tuple = mock(Tuple.class);

        // Ack a tuple
        b.ack(tuple);
        verifyNoMoreInteractions(b);

        // Add an outputcollector
        OutputCollector oc = mock(OutputCollector.class);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        b.ack(tuple);
        verify(oc, times(1)).ack(tuple);
        verifyNoMoreInteractions(oc);
    }

    /**
     * Test a multi ack.
     */
    @Test
    public void testMultiAck() {
        // Various data mocks
        TestBolt b = spy(new TestBolt());
        List<Tuple> tuples = new ArrayList<>();
        Tuple t1 = mock(Tuple.class);
        Tuple t2 = mock(Tuple.class);
        tuples.add(t1);
        tuples.add(t2);

        // Ack a tuple
        b.ack(tuples);
        verifyNoMoreInteractions(b);

        // Add an outputcollector
        OutputCollector oc = mock(OutputCollector.class);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        b.ack(tuples);
        verify(oc, times(1)).ack(t1);
        verify(oc, times(1)).ack(t2);
        verifyNoMoreInteractions(oc);
    }

    /**
     * Test a single Fail.
     */
    @Test
    public void testSingleFail() {
        // Various data mocks
        TestBolt b = spy(new TestBolt());
        Tuple tuple = mock(Tuple.class);

        // fail a tuple
        b.fail(tuple);
        verifyNoMoreInteractions(b);

        // Add an outputcollector
        OutputCollector oc = mock(OutputCollector.class);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        b.fail(tuple);
        verify(oc, times(1)).fail(tuple);
        verifyNoMoreInteractions(oc);
    }

    /**
     * Test a multi Fail.
     */
    @Test
    public void testMultiFail() {
        // Various data mocks
        TestBolt b = spy(new TestBolt());
        List<Tuple> tuples = new ArrayList<>();
        Tuple t1 = mock(Tuple.class);
        Tuple t2 = mock(Tuple.class);
        tuples.add(t1);
        tuples.add(t2);

        // fail a tuple
        b.fail(tuples);
        verifyNoMoreInteractions(b);

        // Add an outputcollector
        OutputCollector oc = mock(OutputCollector.class);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        b.fail(tuples);
        verify(oc, times(1)).fail(t1);
        verify(oc, times(1)).fail(t2);
        verifyNoMoreInteractions(oc);
    }

    /**
     * Test a reportError.
     */
    @Test
    public void testReportError() {
        // Various data mocks
        TestBolt b = spy(new TestBolt());
        Exception e = new Exception();

        // Report an error without an outputcollector.
        b.reportError(e);
        verifyNoMoreInteractions(b);

        // Add an outputcollector
        OutputCollector oc = mock(OutputCollector.class);
        b.prepare(new HashMap<>(),
                mock(TopologyContext.class),
                oc);

        // Send a tuple with an outputcollector
        b.reportError(e);
        verify(oc, times(1)).reportError(e);
        verifyNoMoreInteractions(oc);
    }

    /**
     * A test bolt, used for testing.
     */
    private static class TestBolt extends AbstractBolt {

        @Override
        protected void configure(final Map stormConf,
                                 final TopologyContext context) {

        }

        @Override
        public void execute(final Tuple input) {

        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
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
        public Collection<Stream> calculateEmittedStreams(
                final Collection<Stream> providedStreams) {
            return providedStreams;
        }
    }
}
