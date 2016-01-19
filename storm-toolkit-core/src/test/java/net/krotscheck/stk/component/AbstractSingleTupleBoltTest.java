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

import net.krotscheck.stk.component.exception.BoltProcessingException;
import net.krotscheck.stk.stream.Stream;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test case for the abstract single tuple bolt.
 */
public final class AbstractSingleTupleBoltTest {

    /**
     * Assert a tick tuple is passed to the tick method.
     */
    @Test
    public void testTickTuple() {
        TestBolt tb = spy(new TestBolt());

        // Test input data
        TopologyContext tc = mock(TopologyContext.class);
        OutputCollector oc = mock(OutputCollector.class);

        // Create a mock tick tuple
        Tuple tickTuple = mock(Tuple.class);
        when(tickTuple.getSourceComponent())
                .thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tickTuple.getSourceStreamId())
                .thenReturn(Constants.SYSTEM_TICK_STREAM_ID);

        // Prepare the bolt
        tb.prepare(new HashMap<>(), tc, oc);
        tb.execute(tickTuple);

        verify(tb, times(1)).tick(tickTuple);
        verify(oc, times(1)).ack(tickTuple);
    }

    /**
     * Assert that a tuple is processed.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testProcessTuple() throws Exception {
        TestBolt tb = spy(new TestBolt());

        // Test input data
        TopologyContext tc = mock(TopologyContext.class);
        OutputCollector oc = mock(OutputCollector.class);

        // Create a mock tuple
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent())
                .thenReturn(Constants.COORDINATED_STREAM_ID);
        when(tuple.getSourceStreamId())
                .thenReturn("default");

        // Prepare the bolt
        tb.prepare(new HashMap<>(), tc, oc);
        tb.execute(tuple);

        verify(tb, times(1)).process(tuple);
        verify(oc, times(1)).ack(tuple);
    }

    /**
     * Assert that an error tuple is correctly reported.
     *
     * @throws Exception An unexpected Exception.
     */
    @Test
    public void testErrorTuple() throws Exception {
        TestBolt tb = spy(new TestBolt(true));

        // Test input data
        TopologyContext tc = mock(TopologyContext.class);
        OutputCollector oc = mock(OutputCollector.class);

        // Create a mock tuple
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent())
                .thenReturn(Constants.COORDINATED_STREAM_ID);
        when(tuple.getSourceStreamId())
                .thenReturn("default");

        // Prepare the bolt
        tb.prepare(new HashMap<>(), tc, oc);
        tb.execute(tuple);

        verify(tb, times(1)).process(tuple);
        verify(oc, times(1)).reportError(any(BoltProcessingException.class));
    }

    /**
     * A test bolt, used for testing.
     */
    private static class TestBolt extends AbstractSingleTupleBolt {

        /**
         * Whether to throw an error on process.
         */
        private Boolean throwError = false;

        /**
         * Create a new Test Bolt.
         */
        TestBolt() {

        }

        /**
         * Create a new Test Bolt with an error flag.
         *
         * @param throwError Whether to throw an error.
         */
        TestBolt(final Boolean throwError) {
            this.throwError = throwError;
        }

        /**
         * Process a tick event.
         *
         * @param tuple The tick tuple.
         */
        @Override
        protected void tick(final Tuple tuple) {

        }

        /**
         * Process a single tuple.
         *
         * @param input The tuple to process.
         * @throws BoltProcessingException An exception encountered during bolt
         *                                 processing. This will trigger a
         *                                 reportError() and a fail() to be sent
         *                                 to the outputCollector.
         */
        @Override
        protected void process(final Tuple input)
                throws BoltProcessingException {
            if (throwError) {
                throw new BoltProcessingException();
            }
        }

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
            return null;
        }

        /**
         * Declare configuration specific to this component.
         */
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
