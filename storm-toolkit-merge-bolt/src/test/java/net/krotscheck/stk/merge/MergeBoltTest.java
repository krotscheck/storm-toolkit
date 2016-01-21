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

package net.krotscheck.stk.merge;

import net.krotscheck.stk.component.exception.BoltProcessingException;
import net.krotscheck.stk.stream.Stream;
import net.krotscheck.stk.stream.Type;
import net.krotscheck.stk.test.STKAssert;
import net.krotscheck.stk.test.TestTuple;
import net.krotscheck.stk.test.TestTupleUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for the Merge Bolt.
 *
 * @author Michael Krotscheck
 */
public final class MergeBoltTest {

    /**
     * Private stream name.
     */
    private final String customStreamName = "test_stream_name";

    /**
     * A test output collector.
     */
    private OutputCollector oc;

    /**
     * A test topology context.
     */
    private TopologyContext tc;

    /**
     * List of test streams.
     */
    private List<Stream> providedTestStreams = new ArrayList<>();

    /**
     * Test data for default stream 1.
     */
    private List<SortedMap<String, Object>> testDataDefault1;

    /**
     * Test data for default stream 2.
     */
    private List<SortedMap<String, Object>> testDataDefault2;

    /**
     * Create testing data.
     */
    @Before
    public void setup() {
        Stream inputDefault1 = new Stream.Builder(Utils.DEFAULT_STREAM_ID)
                .addSchemaField("d_1", Type.STRING)
                .addSchemaField("d_2", Type.BOOLEAN)
                .build();
        Stream inputDefault2 = new Stream.Builder(Utils.DEFAULT_STREAM_ID)
                .addSchemaField("d_3", Type.STRING)
                .addSchemaField("d_4", Type.BOOLEAN)
                .build();

        Stream inputCustom1 = new Stream.Builder(customStreamName)
                .addSchemaField("c_1", Type.STRING)
                .addSchemaField("c_2", Type.BOOLEAN)
                .build();
        Stream inputCustom2 = new Stream.Builder(customStreamName)
                .addSchemaField("c_3", Type.STRING)
                .addSchemaField("c_4", Type.BOOLEAN)
                .build();

        providedTestStreams.add(inputDefault1);
        providedTestStreams.add(inputDefault2);
        providedTestStreams.add(inputCustom1);
        providedTestStreams.add(inputCustom2);

        List<Integer> emittedTupleIds = new LinkedList<>();
        emittedTupleIds.add(1);

        oc = mock(OutputCollector.class);
        when(oc.emit(Matchers.eq(Utils.DEFAULT_STREAM_ID),
                Matchers.any(Tuple.class),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);
        when(oc.emit(Matchers.eq(Utils.DEFAULT_STREAM_ID),
                Matchers.anyListOf(Tuple.class),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);

        when(oc.emit(Matchers.eq(customStreamName),
                Matchers.any(Tuple.class),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);
        when(oc.emit(Matchers.eq(customStreamName),
                Matchers.anyListOf(Tuple.class),
                Matchers.anyList()))
                .thenReturn(emittedTupleIds);

        tc = mock(TopologyContext.class);

        testDataDefault1 = new LinkedList<>();
        testDataDefault2 = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            SortedMap<String, Object> testData1 = new TreeMap<>();
            testData1.put("d_1", "Test");
            testData1.put("d_2", true);
            testDataDefault1.add(testData1);

            SortedMap<String, Object> testData2 = new TreeMap<>();
            testData2.put("d_3", "Test");
            testData2.put("d_4", true);
            testDataDefault2.add(testData2);
        }
    }

    /**
     * Clear the testing data.
     */
    @After
    public void teardown() {
        providedTestStreams.clear();
        tc = null;
        oc = null;
        testDataDefault1 = null;
        testDataDefault2 = null;
    }

    /**
     * Assert that this bolt meets the STK basic usage requirements.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testSTKComponent() throws Exception {
        MergeBolt m = new MergeBolt();
        STKAssert.assertReserializable(m);
    }

    /**
     * Assert that the tick() method does nothing.
     */
    @Test
    public void testNoTick() {
        MergeBolt m = new MergeBolt();
        m.prepare(new HashMap<>(), tc, oc);
        m.tick(TestTupleUtil.tickTuple());

        verifyNoMoreInteractions(oc, tc);
    }

    /**
     * Assert that the configure() method does nothing.
     */
    @Test
    public void testNoConfigure() {
        MergeBolt m = new MergeBolt();
        m.configure(new HashMap<>(), tc);

        verifyNoMoreInteractions(tc);
    }

    /**
     * Assert that the getComponentConfiguration() method does nothing.
     */
    @Test
    public void testNoComponentConfiguration() {
        MergeBolt m = new MergeBolt();
        Map<String, Object> conf = m.getComponentConfiguration();
        Assert.assertNotNull(conf);
        Assert.assertEquals(0, conf.size());
    }

    /**
     * Assert that incoming tuples are merged into the new schema and sent on.
     *
     * @throws BoltProcessingException Should not be thrown.
     */
    @Test
    public void testTupleIsMerged() throws BoltProcessingException {
        MergeBolt m = new MergeBolt();
        m.addProvidedStream(providedTestStreams);
        m.prepare(new HashMap<>(), tc, oc);

        for (SortedMap<String, Object> row : testDataDefault1) {
            Tuple t = new TestTuple(row);
            m.process(t);

            SortedMap<String, Object> mergedRow = new TreeMap<>(row);
            mergedRow.put("d_3", null);
            mergedRow.put("d_4", null);
            verify(oc).emit(Utils.DEFAULT_STREAM_ID, t,
                    new LinkedList<>(mergedRow.values()));
        }
    }

    /**
     * Assert that an input tuple with too many fields loses those.
     *
     * @throws BoltProcessingException Should not be thrown.
     */
    @Test
    public void testInputTupleLosesExtraFields()
            throws BoltProcessingException {
        MergeBolt m = new MergeBolt();
        m.addProvidedStream(providedTestStreams);
        m.prepare(new HashMap<>(), tc, oc);

        for (SortedMap<String, Object> row : testDataDefault1) {
            SortedMap<String, Object> bigRow = new TreeMap<>(row);
            bigRow.put("col_x", false);

            Tuple t = new TestTuple(bigRow);
            m.process(t);

            SortedMap<String, Object> mergedRow = new TreeMap<>(row);
            mergedRow.put("d_3", null);
            mergedRow.put("d_4", null);
            verify(oc).emit(Utils.DEFAULT_STREAM_ID, t,
                    new LinkedList<>(mergedRow.values()));
        }
    }

    /**
     * Assert that incoming tuples with no provided streams emit nothing.
     *
     * @throws BoltProcessingException Should not be thrown.
     */
    @Test
    public void testTuplesDroppedWithNoStreams()
            throws BoltProcessingException {
        MergeBolt m = new MergeBolt();
        m.addProvidedStream(providedTestStreams);
        m.prepare(new HashMap<>(), tc, oc);

        for (SortedMap<String, Object> row : testDataDefault1) {
            TestTuple t = new TestTuple(row);
            t.setSourceStreamId("invalid_stream_name");
            m.process(t);

            verifyNoMoreInteractions(oc);
        }
    }

    /**
     * Assert that incoming tuples with no provided streams emit nothing.
     *
     * @throws BoltProcessingException Should not be thrown.
     */
    @Test
    public void testCalculatedEmittedStreamsAreMerged()
            throws BoltProcessingException {
        MergeBolt m = new MergeBolt();
        Stream s1 = new Stream.Builder(Utils.DEFAULT_STREAM_ID)
                .addSchemaField("col_1", Type.BOOLEAN)
                .build();
        Stream s2 = new Stream.Builder(Utils.DEFAULT_STREAM_ID)
                .addSchemaField("col_2", Type.BOOLEAN)
                .build();
        m.addProvidedStream(s1);
        m.addProvidedStream(s2);

        Collection<Stream> emitted = m.getEmittedStreams();
        Assert.assertEquals(1, emitted.size());
    }

    /**
     * Test that hashCode always returns something different.
     */
    @Test
    public void testHashCode() {
        MergeBolt m1 = new MergeBolt();
        MergeBolt m2 = new MergeBolt();

        Assert.assertEquals(m1.hashCode(), m2.hashCode());

        m2.addProvidedStream(providedTestStreams);
        Assert.assertNotEquals(m1.hashCode(), m2.hashCode());
    }

    /**
     * Test that mergeBolts configured separately are nevertheless equal.
     */
    @Test
    public void testEquals() {
        MergeBolt m1 = new MergeBolt();
        MergeBolt m2 = new MergeBolt();
        MergeBolt m3 = new MergeBolt();
        m3.addProvidedStream(providedTestStreams);

        MergeBolt m4 = new MergeBolt();
        m4.addProvidedStream(providedTestStreams);
        m4.addProvidedStream(new Stream.Builder("test")
                .addSchemaField("test", Type.BOOLEAN)
                .build());

        Assert.assertTrue(m1.equals(m1));
        Assert.assertTrue(m1.equals(m2));
        Assert.assertFalse(m1.equals(new Object()));
        Assert.assertFalse(m1.equals(null));
        Assert.assertFalse(m1.equals(m3));
        Assert.assertFalse(m4.equals(m3));
        Assert.assertFalse(m3.equals(m4));
    }
}
