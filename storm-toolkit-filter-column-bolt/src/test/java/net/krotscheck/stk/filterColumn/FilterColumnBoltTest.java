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

package net.krotscheck.stk.filterColumn;

import net.krotscheck.stk.component.exception.BoltProcessingException;
import net.krotscheck.stk.stream.Schema;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * These tests assert that the Filter Column bolt will only emit the columns
 * asked for.
 *
 * @author Michael Krotscheck
 */
public final class FilterColumnBoltTest {

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
                .addSchemaField("d_3", Type.STRING)
                .addSchemaField("d_4", Type.BOOLEAN)
                .build();

        Stream inputCustom1 = new Stream.Builder(customStreamName)
                .addSchemaField("c_1", Type.STRING)
                .addSchemaField("c_2", Type.BOOLEAN)
                .addSchemaField("c_3", Type.STRING)
                .addSchemaField("c_4", Type.BOOLEAN)
                .build();

        providedTestStreams.add(inputDefault1);
        providedTestStreams.add(inputCustom1);

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
        Set<String> filterColumns = new TreeSet<>();
        filterColumns.add("foo");
        filterColumns.add("bar");

        FilterColumnBolt before = new FilterColumnBolt();
        before.setRequestedColumns(filterColumns);

        FilterColumnBolt after = STKAssert.assertReserializable(before);
        Assert.assertEquals(filterColumns, after.getRequestedColumns());
    }

    /**
     * Assert that the tick() method does nothing.
     */
    @Test
    public void testNoTick() {
        FilterColumnBolt m = new FilterColumnBolt();
        m.prepare(new HashMap<>(), tc, oc);
        m.tick(TestTupleUtil.tickTuple());

        verifyNoMoreInteractions(oc, tc);
    }

    /**
     * Assert that the configure() method does nothing.
     */
    @Test
    public void testNoConfigure() {
        FilterColumnBolt m = new FilterColumnBolt();
        m.configure(new HashMap<>(), tc);

        verifyNoMoreInteractions(tc);
    }

    /**
     * Assert that the getComponentConfiguration() method does nothing.
     */
    @Test
    public void testNoComponentConfiguration() {
        FilterColumnBolt m = new FilterColumnBolt();
        Map<String, Object> conf = m.getComponentConfiguration();
        Assert.assertNotNull(conf);
        Assert.assertEquals(0, conf.size());
    }

    /**
     * Assert that the requestedColumns property can be get and set.
     */
    @Test
    public void testRequestedColumnsGetSet() {
        Set<String> filterColumns = new TreeSet<>();
        filterColumns.add("d_1");
        filterColumns.add("d_3");

        FilterColumnBolt m = new FilterColumnBolt();
        Assert.assertNotNull(m.getRequestedColumns());
        Assert.assertEquals(0, m.getRequestedColumns().size());

        m.setRequestedColumns(filterColumns);
        Assert.assertNotNull(m.getRequestedColumns());
        Assert.assertEquals(filterColumns, m.getRequestedColumns());
    }

    /**
     * Assert that the calculated output schema from the bolt only gives us what
     * we're expecting.
     */
    @Test
    public void testOutputSchema() {
        Set<String> filterColumns = new TreeSet<>();
        filterColumns.add("d_1");
        filterColumns.add("c_4");

        FilterColumnBolt m = new FilterColumnBolt();
        m.setRequestedColumns(filterColumns);
        m.addProvidedStream(providedTestStreams);

        Collection<Stream> results = m.getEmittedStreams();

        // There should be two output streams
        Assert.assertEquals(2, results.size());

        // Each output stream should have two columns.
        Schema defaultSchema = m.getEmittedStream(Utils.DEFAULT_STREAM_ID)
                .getSchema();
        Assert.assertEquals(2, defaultSchema.size());
        // d_1 should have populated from the original custom schema, the
        // other should default to String
        Assert.assertEquals(defaultSchema.get("d_1"), Type.STRING);
        Assert.assertEquals(defaultSchema.get("c_4"), Type.STRING);

        Schema customSchema = m.getEmittedStream(customStreamName)
                .getSchema();
        Assert.assertEquals(2, customSchema.size());
        // c_4 should have populated from the original custom schema, the
        // other should default to String
        Assert.assertEquals(customSchema.get("d_1"), Type.STRING);
        Assert.assertEquals(customSchema.get("c_4"), Type.BOOLEAN);
    }

    /**
     * Assert that incoming tuples are merged into the new schema and sent on.
     *
     * @throws BoltProcessingException Should not be thrown.
     */
    @Test
    public void testTupleIsMerged() throws BoltProcessingException {
        Set<String> filterColumns = new TreeSet<>();
        filterColumns.add("d_1");
        filterColumns.add("d_3");

        FilterColumnBolt m = new FilterColumnBolt();
        m.addProvidedStream(providedTestStreams);
        m.setRequestedColumns(filterColumns);
        m.prepare(new HashMap<>(), tc, oc);

        for (SortedMap<String, Object> row : testDataDefault1) {
            Tuple t = new TestTuple(row);
            m.process(t);

            SortedMap<String, Object> filteredRow = new TreeMap<>(row);
            filteredRow.put("d_3", null);
            filteredRow.remove("d_2");
            verify(oc).emit(Utils.DEFAULT_STREAM_ID, t,
                    new LinkedList<>(filteredRow.values()));
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
        FilterColumnBolt m = new FilterColumnBolt();
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
     * Test that hashCode always returns something different.
     */
    @Test
    public void testHashCode() {
        Set<String> filterColumns = new TreeSet<>();
        filterColumns.add("d_1");
        filterColumns.add("d_3");
        FilterColumnBolt m1 = new FilterColumnBolt();
        FilterColumnBolt m2 = new FilterColumnBolt();
        Assert.assertEquals(m1.hashCode(), m2.hashCode());

        m1.setRequestedColumns(filterColumns);
        Assert.assertNotEquals(m1.hashCode(), m2.hashCode());

        m2.setRequestedColumns(filterColumns);
        Assert.assertEquals(m1.hashCode(), m2.hashCode());

        m2.addProvidedStream(providedTestStreams);
        Assert.assertEquals(m1.hashCode(), m2.hashCode());
    }

    /**
     * Test that FilterColumnBolts configured separately are nevertheless
     * equal.
     */
    @Test
    public void testEquals() {
        Set<String> filterColumns = new TreeSet<>();
        filterColumns.add("d_1");
        filterColumns.add("d_3");
        FilterColumnBolt m1 = new FilterColumnBolt();
        FilterColumnBolt m2 = new FilterColumnBolt();
        FilterColumnBolt m3 = new FilterColumnBolt();
        m3.addProvidedStream(providedTestStreams);

        Assert.assertTrue(m1.equals(m1));
        Assert.assertTrue(m1.equals(m2));

        m1.setRequestedColumns(filterColumns);
        Assert.assertFalse(m1.equals(m2));

        m2.setRequestedColumns(filterColumns);
        Assert.assertTrue(m1.equals(m2));

        Assert.assertFalse(m1.equals(new Object()));
        Assert.assertFalse(m1.equals(null));
        Assert.assertFalse(m1.equals(m3));
    }
}
