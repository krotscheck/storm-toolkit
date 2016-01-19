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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.topology.OutputFieldsDeclarer;

import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the stream processor.
 */
public final class AbstractStreamProcessorTest {

    /**
     * Assert that the abstract bolt is abstract.
     *
     * @throws Exception Unexpected Exceptions.
     */
    @Test(expected = InstantiationException.class)
    public void testAbstractClass() throws Exception {
        AbstractStreamProcessor.class.newInstance();
    }

    /**
     * Assert that streams calculated by an implementing can be accessed
     * individually.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testGetStreamByName() throws Exception {
        TestProcessor bolt = Mockito.spy(new TestProcessor());

        Stream s1 = new Builder("stream1")
                .addSchemaField("test1", Type.BOOLEAN)
                .build();
        Stream s2 = new Builder("stream2")
                .addSchemaField("test2", Type.BOOLEAN)
                .build();
        Set<Stream> testSet = new HashSet<>();
        testSet.add(s1);
        testSet.add(s2);

        when(bolt.calculateEmittedStreams(anySetOf(Stream.class)))
                .thenReturn(testSet);

        // Trigger initialization.
        bolt.invalidateEmittedStreams();

        Stream emitted1 = bolt.getEmittedStream("stream1");
        Stream emitted2 = bolt.getEmittedStream("stream2");

        Assert.assertSame(emitted1, s1);
        Assert.assertSame(emitted2, s2);
    }

    /**
     * Assert that streams calculated by an implementing class are available.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testCalculatedEmittedStreams() throws Exception {
        TestProcessor bolt = Mockito.spy(new TestProcessor());
        Assert.assertFalse(bolt.hasEmittedStreams());

        Stream s1 = new Builder("stream1")
                .addSchemaField("test1", Type.BOOLEAN)
                .build();
        Stream s2 = new Builder("stream2")
                .addSchemaField("test2", Type.BOOLEAN)
                .build();
        Set<Stream> testSet = new HashSet<>();
        testSet.add(s1);
        testSet.add(s2);

        when(bolt.calculateEmittedStreams(anySetOf(Stream.class)))
                .thenReturn(testSet);

        // Trigger initialization.
        bolt.invalidateEmittedStreams();

        Collection<Stream> emitted = bolt.getEmittedStreams();
        Assert.assertTrue(bolt.hasEmittedStreams());
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
        TestProcessor bolt = Mockito.spy(new TestProcessor());

        when(bolt.calculateEmittedStreams(anySetOf(Stream.class)))
                .thenReturn(null);

        // Trigger initialization.
        bolt.invalidateEmittedStreams();

        Collection<Stream> emitted = bolt.getEmittedStreams();

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
        TestProcessor bolt = Mockito.spy(new TestProcessor());

        Stream s1 = new Builder("stream1")
                .addSchemaField("test1", Type.BOOLEAN)
                .build();
        Stream s2 = new Builder("stream1")
                .addSchemaField("test2", Type.BOOLEAN)
                .build();
        Set<Stream> testSet = new HashSet<>();
        testSet.add(s1);
        testSet.add(s2);

        when(bolt.calculateEmittedStreams(anySetOf(Stream.class)))
                .thenReturn(testSet);

        // Trigger initialization.
        bolt.invalidateEmittedStreams();

        Collection<Stream> emitted = bolt.getEmittedStreams();
        Assert.assertEquals(1, emitted.size());
        Stream one = bolt.getEmittedStream("stream1");
        Assert.assertEquals(Type.BOOLEAN, one.getSchema().get("test1"));
        Assert.assertEquals(Type.BOOLEAN, one.getSchema().get("test2"));
    }

    /**
     * Assert that getting the emitted streams returns an unmodifiable.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testGetEmittedStreamsUnmodifiable() {
        IStreamEmitter emitter = new TestProcessor();
        Collection<Stream> emitted = emitter.getEmittedStreams();
        Assert.assertNotNull(emitted);
        Stream s = new Builder("test").build();
        emitted.add(s);
    }

    /**
     * Assert that get provided streams returns an unmodifiable.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testGetProvidedStreamsUnmodifiable() {
        IStreamConsumer consumer = new TestProcessor();
        Collection<Stream> provided = consumer.getProvidedStreams();
        Assert.assertNotNull(provided);
        Stream s = new Builder("test").build();
        provided.add(s);
    }

    /**
     * Assert that hasProvidedStreams is accurate.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testHasProvidedStreams() throws Exception {
        IStreamConsumer consumer = new TestProcessor();
        Assert.assertFalse(consumer.hasProvidedStreams());

        Stream s = new Builder("test").build();
        consumer.addProvidedStream(s);

        Assert.assertTrue(consumer.hasProvidedStreams());
    }

    /**
     * Assert that testAddProvidedStream functions with a stream.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddProvidedStream() throws Exception {
        IStreamConsumer consumer = new TestProcessor();
        Assert.assertFalse(consumer.hasProvidedStreams());

        Stream s = new Builder("test").build();
        consumer.addProvidedStream(s);

        Collection<Stream> provided = consumer.getProvidedStreams();

        Assert.assertEquals(1, provided.size());
        Assert.assertTrue(provided.contains(s));
    }

    /**
     * Assert that testAddMultipleIdenticalStreams.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddMerge() throws Exception {
        IStreamConsumer consumer = new TestProcessor();
        Assert.assertFalse(consumer.hasProvidedStreams());

        Stream s1 = new Builder("default")
                .addSchemaField("col_1", Type.BOOLEAN)
                .addSchemaField("col_2", Type.BYTE)
                .build();
        Stream s2 = new Builder("default")
                .addSchemaField("col_1", Type.BOOLEAN)
                .addSchemaField("col_3", Type.DOUBLE)
                .build();

        consumer.addProvidedStream(s1);
        consumer.addProvidedStream(s2);

        Collection<Stream> provided = consumer.getProvidedStreams();

        Assert.assertEquals(1, provided.size());
        Stream[] results = provided.toArray(new Stream[provided.size()]);

        Stream result = results[0];
        Schema resultSchema = result.getSchema();

        Assert.assertEquals("default", result.getStreamId());
        Assert.assertEquals(Type.BOOLEAN, resultSchema.get("col_1"));
        Assert.assertEquals(Type.BYTE, resultSchema.get("col_2"));
        Assert.assertEquals(Type.DOUBLE, resultSchema.get("col_3"));
    }

    /**
     * Assert that adding multiple conflicting streams overwrites previous
     * items.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddMergeConflicting() throws Exception {
        IStreamConsumer consumer = new TestProcessor();
        Assert.assertFalse(consumer.hasProvidedStreams());

        Stream s1 = new Builder("default")
                .addSchemaField("col_1", Type.BOOLEAN)
                .addSchemaField("col_2", Type.BYTE)
                .build();
        Stream s2 = new Builder("default")
                .addSchemaField("col_1", Type.INT)
                .addSchemaField("col_3", Type.DOUBLE)
                .build();

        consumer.addProvidedStream(s1);
        consumer.addProvidedStream(s2);

        Collection<Stream> provided = consumer.getProvidedStreams();

        Assert.assertEquals(1, provided.size());
        Stream[] results = provided.toArray(new Stream[provided.size()]);

        Stream result = results[0];
        Schema resultSchema = result.getSchema();

        Assert.assertEquals("default", result.getStreamId());
        Assert.assertEquals(Type.INT, resultSchema.get("col_1"));
        Assert.assertEquals(Type.BYTE, resultSchema.get("col_2"));
        Assert.assertEquals(Type.DOUBLE, resultSchema.get("col_3"));
    }

    /**
     * Assert that testAddProvidedStream functions with a stream collection.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddProvidedStreamCollection() throws Exception {
        IStreamConsumer consumer = new TestProcessor();
        Assert.assertFalse(consumer.hasProvidedStreams());

        Stream s1 = new Builder("test1").build();
        Stream s2 = new Builder("test2").build();

        List<Stream> streams = new ArrayList<>();
        streams.add(s1);
        streams.add(s2);

        consumer.addProvidedStream(streams);

        Collection<Stream> provided = consumer.getProvidedStreams();

        Assert.assertEquals(2, provided.size());
        Assert.assertTrue(provided.contains(s1));
        Assert.assertTrue(provided.contains(s2));
    }

    /**
     * Assert that testAddProvidedStream functions with an emitter.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddProvidedStreamEmitter() throws Exception {
        IStreamConsumer consumer = new TestProcessor();

        Stream s1 = new Builder("test1").build();
        Stream s2 = new Builder("test2").build();

        Set<Stream> streams = new HashSet<>();
        streams.add(s1);
        streams.add(s2);

        IStreamEmitter emitter = mock(IStreamEmitter.class);
        when(emitter.getEmittedStreams()).thenReturn(streams);

        Assert.assertFalse(consumer.hasProvidedStreams());
        consumer.addProvidedStream(emitter);

        Collection<Stream> provided = consumer.getProvidedStreams();

        Assert.assertEquals(2, provided.size());
        Assert.assertTrue(provided.contains(s1));
        Assert.assertTrue(provided.contains(s2));
    }

    /**
     * Assert that clearProvidedStreams functions.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testClearProvidedStreams() throws Exception {
        IStreamConsumer consumer = new TestProcessor();
        Assert.assertFalse(consumer.hasProvidedStreams());

        Stream s = new Builder("test").build();
        consumer.addProvidedStream(s);
        Assert.assertTrue(consumer.hasProvidedStreams());

        consumer.clearProvidedStreams();
        Assert.assertFalse(consumer.hasProvidedStreams());
    }

    /**
     * Assert that output fields are declared from the calculated schema.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testDeclareOutputFields() throws Exception {
        TestProcessor bolt = new TestProcessor();
        Assert.assertFalse(bolt.hasProvidedStreams());

        Stream s1 = new Builder("default")
                .addSchemaField("col_1", Type.BOOLEAN)
                .addSchemaField("col_2", Type.BYTE)
                .build();
        Stream s2 = new Builder("default")
                .addSchemaField("col_1", Type.BOOLEAN)
                .addSchemaField("col_3", Type.DOUBLE)
                .build();

        bolt.addProvidedStream(s1);
        bolt.addProvidedStream(s2);

        // Get the calculated provided schema for later.
        Collection<Stream> provided = bolt.getProvidedStreams();
        Assert.assertEquals(1, provided.size());
        Stream[] results = provided.toArray(new Stream[provided.size()]);
        Stream result = results[0];

        OutputFieldsDeclarer d = mock(OutputFieldsDeclarer.class);
        bolt.declareOutputFields(d);

        verify(d).declareStream("default", false, result.getFields());
    }

    /**
     * A test processor, used for testing, in tests.
     */
    private static class TestProcessor extends AbstractStreamProcessor {

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
        protected Collection<Stream> calculateEmittedStreams(
                final Collection<Stream> providedStreams) {
            return providedStreams;
        }
    }
}
