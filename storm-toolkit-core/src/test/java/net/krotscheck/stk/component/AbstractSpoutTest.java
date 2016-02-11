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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test suite for the AbstractSpout Base Class.
 *
 * @author Michael Krotscheck
 */
public final class AbstractSpoutTest {

    /**
     * Private stream name.
     */
    private final String testStreamName = "test_stream_name";

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
     * A list of emitted streams.
     */
    private List<Stream> emittedStreams;

    /**
     * A test output collector.
     */
    private SpoutOutputCollector oc;

    /**
     * Create some test data.
     */
    @Before
    public void setup() {
        outputTupleMap = new TreeMap<>();
        outputTupleMap.put("col_0", false);
        outputTupleMap.put("col_1", 1);
        outputTupleMap.put("col_2", "two");

        outputTuple = new LinkedList<>(outputTupleMap.values());

        emittedTupleIds = new LinkedList<>();
        emittedTupleIds.add(1);

        oc = mock(SpoutOutputCollector.class);
        when(oc.emit(Matchers.eq(Utils.DEFAULT_STREAM_ID),
                Matchers.anyList(),
                Matchers.anyString()))
                .thenReturn(emittedTupleIds);

        when(oc.emit(Matchers.eq(testStreamName),
                Matchers.anyList(),
                Matchers.anyString()))
                .thenReturn(emittedTupleIds);

        emittedStreams = new LinkedList<>();
        Stream s1 = new Stream.Builder(Utils.DEFAULT_STREAM_ID)
                .addSchemaField("col_0", Type.BOOLEAN)
                .addSchemaField("col_1", Type.INT)
                .addSchemaField("col_2", Type.STRING)
                .build();
        Stream s2 = new Stream.Builder(testStreamName)
                .addSchemaField("col_0", Type.BOOLEAN)
                .addSchemaField("col_1", Type.INT)
                .addSchemaField("col_2", Type.STRING)
                .build();
        emittedStreams.add(s1);
        emittedStreams.add(s2);
    }

    /**
     * Tear down the test data.
     */
    @After
    public void teardown() {
        outputTupleMap = null;
        outputTuple = null;
        emittedTupleIds = null;
        oc = null;
        emittedStreams = null;
    }

    /**
     * Assert that the abstract spout is abstract.
     *
     * @throws Exception Unexpected Exceptions.
     */
    @Test(expected = InstantiationException.class)
    public void testAbstractClass() throws Exception {
        AbstractSpout.class.newInstance();
    }

    /**
     * Assert that the close() method is implemented and final. Since this
     * method is not guaranteed to be invoked, we remove it from our API.
     */
    @Test
    public void testCloseImplemented() {
        for (Method f : AbstractSpout.class
                .getDeclaredMethods()) {
            if (f.getName().equals("close")) {
                Assert.assertTrue(Modifier.isFinal(f.getModifiers()));
                return;
            }
        }
        Assert.assertFalse("method close() is not defined", true);
    }

    /**
     * Assert that prepare calls invalidate the lifecycle.
     */
    @Test
    public void testLifecyclePrepare() {
        TestSpout b = spy(new TestSpout());

        // Assert that prepare calls invalidate
        b.open(new HashMap<>(), mock(TopologyContext.class), oc);
        verify(b, times(1)).invalidate();
    }

    /**
     * Assert that invalidate calls configure() and invalidateEmittedStreams().
     */
    @Test
    public void testLifecycleInvalidate() {
        TestSpout b = spy(new TestSpout());

        // Assert that prepare calls invalidate
        b.invalidate();
        verify(b, times(1)).configure(null, null);
        verify(b, times(1)).calculateEmittedStreams();
    }

    /**
     * Assert that the lifecycle is properly invoked.
     */
    @Test
    public void testCleanupInvalidate() {
        TestSpout b = spy(new TestSpout());

        // Assert that prepare calls invalidate
        b.close();
        verify(b, times(1)).invalidate();
    }

    /**
     * Test emit with the default stream and multiple anchors.
     */
    @Test
    public void testEmitDefaultStream() {
        TestSpout b = spy(new TestSpout(emittedStreams));

        // Send a tuple with no outputcollector
        String messageId = UUID.randomUUID().toString();
        List<Integer> results = b.emit(outputTupleMap, messageId);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        // Add an outputcollector
        b.open(new HashMap<>(), mock(TopologyContext.class), oc);

        // Send a tuple with an outputcollector
        List<Integer> resultsOc = b.emit(outputTupleMap, messageId);
        Assert.assertNotNull(resultsOc);
        Assert.assertEquals(1, resultsOc.size());
        verify(oc, times(1))
                .emit(Utils.DEFAULT_STREAM_ID, outputTuple, messageId);
    }

    /**
     * Test emit with a custom stream and multiple anchors.
     */
    @Test
    public void testEmitCustomStream() {
        TestSpout b = spy(new TestSpout(emittedStreams));

        // Send a tuple with no outputcollector
        String messageId = UUID.randomUUID().toString();
        List<Integer> results = b.emit(testStreamName, outputTupleMap,
                messageId);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        // Add an outputcollector
        b.open(new HashMap<>(), mock(TopologyContext.class), oc);

        // Send a tuple with an outputcollector
        List<Integer> resultsOc = b.emit(testStreamName, outputTupleMap,
                messageId);
        Assert.assertNotNull(resultsOc);
        Assert.assertEquals(1, resultsOc.size());
        verify(oc, times(1))
                .emit(testStreamName, outputTuple, messageId);

    }

    /**
     * Test emit with a custom stream and multiple anchors.
     */
    @Test
    public void testEmitInvalidStream() {
        TestSpout b = spy(new TestSpout(emittedStreams));

        // Send a tuple without an outputCollector
        String messageId = UUID.randomUUID().toString();
        List<Integer> resultNoOc =
                b.emit("bad_stream_name", outputTupleMap, messageId);
        Assert.assertNotNull(resultNoOc);
        Assert.assertEquals(0, resultNoOc.size());

        // Add an outputcollector
        b.open(new HashMap<>(), mock(TopologyContext.class), oc);
        List<Integer> results =
                b.emit("bad_stream_name", outputTupleMap, messageId);
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        verifyNoMoreInteractions(oc);
    }

    /**
     * Test emit with an incomplete tuple.
     */
    @Test
    public void testEmitIncompleteTuple() {
        TestSpout b = spy(new TestSpout(emittedStreams));
        b.open(new HashMap<>(), mock(TopologyContext.class), oc);

        Map<String, Object> testMap = new TreeMap<>(outputTupleMap);
        testMap.remove("col_1");
        Map<String, Object> emittedMap = new TreeMap<>(testMap);
        emittedMap.put("col_1", null);
        List<Object> emittedTuple = new LinkedList<>(emittedMap.values());

        // Send a tuple
        String messageId = UUID.randomUUID().toString();
        List<Integer> results =
                b.emit(testMap, messageId);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        verify(oc, times(1)).emit(Utils.DEFAULT_STREAM_ID, emittedTuple,
                messageId);
    }

    /**
     * Test a reportError.
     */
    @Test
    public void testReportError() {
        // Various data mocks
        TestSpout b = spy(new TestSpout());
        Exception e = new Exception();

        // Report an error without an outputcollector.
        b.reportError(e);
        verifyNoMoreInteractions(b);

        // Add an outputcollector
        b.open(new HashMap<>(), mock(TopologyContext.class), oc);

        // Send a tuple with an outputcollector
        b.reportError(e);
        verify(oc, times(1)).reportError(e);
        verifyNoMoreInteractions(oc);
    }

    /**
     * A test spout, used for testing.
     */
    private static class TestSpout extends AbstractSpout {

        /**
         * The list of emitted streams.
         */
        private List<Stream> emittedStreams = new LinkedList<>();

        /**
         * Create a new instance.
         */
        TestSpout() {
        }

        /**
         * Create a new instance with emitted streams.
         *
         * @param emittedStreams List of emitted streams.
         */
        TestSpout(final List<Stream> emittedStreams) {
            this.emittedStreams.addAll(emittedStreams);
            this.invalidate();
        }

        /**
         * Update this spout's configuration based on the storm configuration
         * and the topology context.
         *
         * @param stormConf The Storm configuration for this spout. This is the
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
         * @return A list of emitted streams.
         */
        @Override
        protected Collection<Stream> calculateEmittedStreams() {
            return emittedStreams;
        }

        /**
         * Declare configuration specific to this component.
         */
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return Collections.emptyMap();
        }

        /**
         * Called when a spout has been activated out of a deactivated mode.
         * nextTuple will be called on this spout soon. A spout can become
         * activated after having been deactivated when the topology is
         * manipulated using the `storm` client.
         */
        @Override
        public void activate() {
            // Do nothing.
        }

        /**
         * Called when a spout has been deactivated. nextTuple will not be
         * called while a spout is deactivated. The spout may or may not be
         * reactivated in the future.
         */
        @Override
        public void deactivate() {
            // Do nothing.
        }

        /**
         * When this method is called, Storm is requesting that the Spout emit
         * tuples to the output collector. This method should be non-blocking,
         * so if the Spout has no tuples to emit, this method should return.
         * nextTuple, ack, and fail are all called in a tight loop in a single
         * thread in the spout task. When there are no tuples to emit, it is
         * courteous to have nextTuple sleep for a short amount of time (like a
         * single millisecond) so as not to waste too much CPU.
         */
        @Override
        public void nextTuple() {
            // Do nothing.
        }

        /**
         * Storm has determined that the tuple emitted by this spout with the
         * msgId identifier has been fully processed. Typically, an
         * implementation of this method will take that message off the queue
         * and prevent it from being replayed.
         */
        @Override
        public void ack(final Object msgId) {
            // Do nothing.
        }

        /**
         * The tuple emitted by this spout with the msgId identifier has failed
         * to be fully processed. Typically, an implementation of this method
         * will put that message back on the queue to be replayed at a later
         * time.
         */
        @Override
        public void fail(final Object msgId) {
            // Do nothing.
        }
    }
}
