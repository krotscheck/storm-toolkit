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

package net.krotscheck.stk.stream;

import net.krotscheck.stk.stream.Stream.Builder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.topology.OutputFieldsDeclarer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the stream emitter.
 */
public final class AbstractStreamEmitterTest {

    /**
     * Assert that the abstract class is abstract.
     *
     * @throws Exception Unexpected Exceptions.
     */
    @Test(expected = InstantiationException.class)
    public void testAbstractClass() throws Exception {
        AbstractStreamEmitter.class.newInstance();
    }

    /**
     * Assert that streams calculated by an implementing can be accessed
     * individually.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testGetStreamByName() throws Exception {
        TestEmitter spout = Mockito.spy(new TestEmitter());

        Stream s1 = new Builder("stream1")
                .addSchemaField("test1", Type.BOOLEAN)
                .build();
        Stream s2 = new Builder("stream2")
                .addSchemaField("test2", Type.BOOLEAN)
                .build();
        Set<Stream> testSet = new HashSet<>();
        testSet.add(s1);
        testSet.add(s2);

        when(spout.calculateEmittedStreams())
                .thenReturn(testSet);

        // Trigger initialization.
        spout.invalidateEmittedStreams();

        Stream emitted1 = spout.getEmittedStream("stream1");
        Stream emitted2 = spout.getEmittedStream("stream2");

        Assert.assertSame(emitted1, s1);
        Assert.assertSame(emitted2, s2);
    }

    /**
     * Assert that output fields are declared from the calculated schema.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testDeclareOutputFields() throws Exception {
        Stream s1 = new Builder("default")
                .addSchemaField("col_1", Type.BOOLEAN)
                .addSchemaField("col_2", Type.BYTE)
                .addSchemaField("col_3", Type.DOUBLE)
                .build();

        List<Stream> emitted = new ArrayList<>();
        emitted.add(s1);
        TestEmitter spout = new TestEmitter(emitted);
        OutputFieldsDeclarer d = mock(OutputFieldsDeclarer.class);
        spout.declareOutputFields(d);

        verify(d).declareStream("default", false, s1.getFields());
    }

    /**
     * Assert that streams calculated by an implementing class are available.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testCalculatedEmittedStreams() throws Exception {
        TestEmitter spout = Mockito.spy(new TestEmitter());
        Assert.assertFalse(spout.hasEmittedStreams());

        Stream s1 = new Builder("stream1")
                .addSchemaField("test1", Type.BOOLEAN)
                .build();
        Stream s2 = new Builder("stream2")
                .addSchemaField("test2", Type.BOOLEAN)
                .build();
        Set<Stream> testSet = new HashSet<>();
        testSet.add(s1);
        testSet.add(s2);

        when(spout.calculateEmittedStreams())
                .thenReturn(testSet);

        // Trigger initialization.
        spout.invalidateEmittedStreams();

        Collection<Stream> emitted = spout.getEmittedStreams();
        Assert.assertTrue(spout.hasEmittedStreams());
        Assert.assertNotNull(emitted);
        Assert.assertEquals(2, emitted.size());
        Assert.assertTrue(emitted.contains(s1));
        Assert.assertTrue(emitted.contains(s2));
    }

    /**
     * Assert that streams calculated by an implementing class are not
     * modifiable.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testCalculatedEmittedStreamUnmodifiable() {
        TestEmitter spout = Mockito.spy(new TestEmitter());

        when(spout.calculateEmittedStreams())
                .thenReturn(null);

        // Trigger initialization.
        spout.invalidateEmittedStreams();

        Collection<Stream> emitted = spout.getEmittedStreams();

        Stream s = new Builder("stream").build();
        emitted.add(s);
    }

    /**
     * Assert that improperly merging streams causes the first one to win.
     *
     * @throws Exception Thrown when it tries to merge streams.
     */
    @Test
    public void testCalculatedEmittedStreamMergeConflict() throws Exception {
        TestEmitter spout = Mockito.spy(new TestEmitter());

        Stream s1 = new Builder("stream1")
                .addSchemaField("test1", Type.BOOLEAN)
                .build();
        Stream s2 = new Builder("stream1")
                .addSchemaField("test2", Type.BOOLEAN)
                .build();
        Set<Stream> testSet = new HashSet<>();
        testSet.add(s1);
        testSet.add(s2);

        when(spout.calculateEmittedStreams())
                .thenReturn(testSet);

        // Trigger initialization.
        spout.invalidateEmittedStreams();

        Collection<Stream> emitted = spout.getEmittedStreams();
        Assert.assertEquals(1, emitted.size());
        Stream one = spout.getEmittedStream("stream1");
        Assert.assertEquals(Type.BOOLEAN, one.getSchema().get("test1"));
        Assert.assertEquals(Type.BOOLEAN, one.getSchema().get("test2"));
    }

    /**
     * Assert that getting the emitted streams returns an unmodifiable.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testGetEmittedStreamsUnmodifiable() {
        IStreamEmitter emitter = new TestEmitter();
        Collection<Stream> emitted = emitter.getEmittedStreams();
        Assert.assertNotNull(emitted);
        Stream s = new Builder("test").build();
        emitted.add(s);
    }

    /**
     * A test Emitter, used for testing, in tests.
     */
    private static class TestEmitter extends AbstractStreamEmitter {

        /**
         * Internal list of emitted streams.
         */
        private final List<Stream> emitted = new ArrayList<>();

        /**
         * Create a new emitter.
         */
        TestEmitter() {
        }

        /**
         * Create a new emitter with provided output streams.
         *
         * @param emittedStreams Emitted streams
         */
        TestEmitter(final List<Stream> emittedStreams) {
            emitted.addAll(emittedStreams);
            invalidateEmittedStreams();
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        /**
         * Whenever the provided streams are changed, this method is invoked to
         * trigger the component to recalculate the emitted streams.
         *
         * @return A list of emitted streams.
         */
        @Override
        protected Collection<Stream> calculateEmittedStreams() {
            return Collections.unmodifiableList(emitted);
        }
    }
}
