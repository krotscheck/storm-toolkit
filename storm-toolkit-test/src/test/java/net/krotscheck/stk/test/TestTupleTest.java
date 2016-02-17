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

package net.krotscheck.stk.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.utils.Utils;

/**
 * Unit test for the test tuple.
 *
 * @author Michael Krotscheck
 */
public final class TestTupleTest {

    /**
     * Some common test data. Clone if you're going to use this.
     */
    private SortedMap<String, Object> testData;

    /**
     * Create some testing classes.
     *
     * @throws Exception Should never be thrown.
     */
    @Before
    public void setUp() throws Exception {
        testData = new TreeMap<>();
        testData.put("integer", 1);
        testData.put("string", "1");
        testData.put("boolean", true);
        testData.put("float", Float.valueOf("1.1"));
        testData.put("short", Short.valueOf("1"));
        testData.put("long", Long.valueOf("11111"));
        testData.put("byte", Byte.valueOf("1"));
        testData.put("double", Double.valueOf("1.111"));
        testData.put("binary", String.valueOf("string").getBytes());
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
     * Assert that the global stream ID is set, and modified, accordingly.
     */
    @Test
    public void testGetSetSourceGlobalStreamid() {
        TestTuple t = new TestTuple(testData);

        Assert.assertEquals("1",
                t.getSourceGlobalStreamid().get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID,
                t.getSourceGlobalStreamid().get_streamId());

        t.setSourceStreamId(Constants.SYSTEM_TICK_STREAM_ID);
        Assert.assertEquals("1",
                t.getSourceGlobalStreamid().get_componentId());
        Assert.assertEquals(Constants.SYSTEM_TICK_STREAM_ID,
                t.getSourceGlobalStreamid().get_streamId());

        t.setSourceComponent("2");
        Assert.assertEquals("2",
                t.getSourceGlobalStreamid().get_componentId());
        Assert.assertEquals(Constants.SYSTEM_TICK_STREAM_ID,
                t.getSourceGlobalStreamid().get_streamId());

    }

    /**
     * Test setting the source component.
     */
    @Test
    public void testGetSetSourceComponent() {
        TestTuple t = new TestTuple(testData);

        Assert.assertEquals("1", t.getSourceComponent());
        t.setSourceComponent("2");
        Assert.assertEquals("2", t.getSourceComponent());
    }

    /**
     * Test updating the source task.
     */
    @Test
    public void testGetSetSourceTask() {
        TestTuple t = new TestTuple(testData);

        Assert.assertEquals(0, t.getSourceTask());
        t.setSourceTask(2);
        Assert.assertEquals(2, t.getSourceTask());
    }

    /**
     * Test updating the source stream ID.
     */
    @Test
    public void testGetSetSourceStreamId() {
        TestTuple t = new TestTuple(testData);

        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, t.getSourceStreamId());
        t.setSourceStreamId("source_stream_id");
        Assert.assertEquals("source_stream_id", t.getSourceStreamId());
    }

    /**
     * Test manipulate the message id.
     */
    @Test
    public void testGetSetMessageId() {
        TestTuple t = new TestTuple(testData);

        MessageId m = t.getMessageId();
        Assert.assertNotNull(m);

        MessageId m2 = MessageId.makeUnanchored();
        Assert.assertNotSame(m, m2);
        t.setMessageId(m2);
        Assert.assertSame(m2, t.getMessageId());
    }

    /**
     * Test the size of the tuple.
     */
    @Test
    public void testSize() {
        TestTuple t = new TestTuple(testData);
        Assert.assertEquals(testData.size(), t.size());
    }

    /**
     * Test contains.
     */
    @Test
    public void testContains() {
        TestTuple t = new TestTuple(testData);
        Assert.assertTrue(t.contains("integer"));
        Assert.assertFalse(t.contains("invalid"));
    }

    /**
     * Test retrieve the fields.
     */
    @Test
    public void testGetFields() {
        TestTuple t = new TestTuple(testData);
        Fields f = t.getFields();
        Assert.assertTrue(f.contains("integer"));
        Assert.assertTrue(f.contains("string"));
        Assert.assertTrue(f.contains("boolean"));
        Assert.assertEquals(testData.size(), f.size());
    }

    /**
     * Test the field index method.
     */
    @Test
    public void testFieldIndex() {
        TestTuple t = new TestTuple(testData);

        // Sorted.
        Assert.assertEquals(0, t.fieldIndex("binary"));
        Assert.assertEquals(1, t.fieldIndex("boolean"));
        Assert.assertEquals(2, t.fieldIndex("byte"));
        Assert.assertEquals(3, t.fieldIndex("double"));
        Assert.assertEquals(4, t.fieldIndex("float"));
        Assert.assertEquals(5, t.fieldIndex("integer"));
        Assert.assertEquals(6, t.fieldIndex("long"));
        Assert.assertEquals(7, t.fieldIndex("short"));
        Assert.assertEquals(8, t.fieldIndex("string"));
    }

    /**
     * Test the select method.
     */
    @Test
    public void testSelect() {
        TestTuple t = new TestTuple(testData);

        Fields f = new Fields("integer", "string");
        List<Object> results = t.select(f);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(1, results.get(0));
        Assert.assertEquals("1", results.get(1));
    }

    /**
     * Test retrieving the value.
     */
    @Test
    public void testGetValue() {
        TestTuple t = new TestTuple(testData);

        Assert.assertEquals(true, t.getValue(1));
        Assert.assertEquals(true, t.getValueByField("boolean"));
    }

    /**
     * Test casting a value to string.
     */
    @Test
    public void testGetString() {
        TestTuple t = new TestTuple(testData);
        String key = "string";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getString(index) instanceof String);
        Assert.assertEquals(testData.get(key), t.getString(index));
        Assert.assertTrue(t.getStringByField(key) instanceof String);
        Assert.assertEquals(testData.get(key), t.getStringByField(key));
    }

    /**
     * Test getting an integer value.
     */
    @Test
    public void testGetInteger() {
        TestTuple t = new TestTuple(testData);
        String key = "integer";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getInteger(index) instanceof Integer);
        Assert.assertEquals(testData.get(key), t.getInteger(index));
        Assert.assertTrue(t.getIntegerByField(key) instanceof Integer);
        Assert.assertEquals(testData.get(key), t.getIntegerByField(key));
    }

    /**
     * Test getting a long value.
     */
    @Test
    public void testGetLong() {
        TestTuple t = new TestTuple(testData);
        String key = "long";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getLong(index) instanceof Long);
        Assert.assertEquals(testData.get(key), t.getLong(index));
        Assert.assertTrue(t.getLongByField(key) instanceof Long);
        Assert.assertEquals(testData.get(key), t.getLongByField(key));
    }

    /**
     * Test getting a boolean value.
     */
    @Test
    public void testGetBoolean() {
        TestTuple t = new TestTuple(testData);
        String key = "boolean";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getBoolean(index) instanceof Boolean);
        Assert.assertEquals(testData.get(key), t.getBoolean(index));
        Assert.assertTrue(t.getBooleanByField(key) instanceof Boolean);
        Assert.assertEquals(testData.get(key), t.getBooleanByField(key));
    }

    /**
     * Test getting a short value.
     */
    @Test
    public void testGetShort() {
        TestTuple t = new TestTuple(testData);
        String key = "short";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getShort(index) instanceof Short);
        Assert.assertEquals(testData.get(key), t.getShort(index));
        Assert.assertTrue(t.getShortByField(key) instanceof Short);
        Assert.assertEquals(testData.get(key), t.getShortByField(key));
    }

    /**
     * Test getting a byte value.
     */
    @Test
    public void testGetByte() {
        TestTuple t = new TestTuple(testData);
        String key = "byte";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getByte(index) instanceof Byte);
        Assert.assertEquals(testData.get(key), t.getByte(index));
        Assert.assertTrue(t.getByteByField(key) instanceof Byte);
        Assert.assertEquals(testData.get(key), t.getByteByField(key));
    }

    /**
     * Test getting a double value.
     */
    @Test
    public void testGetDouble() {
        TestTuple t = new TestTuple(testData);
        String key = "double";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getDouble(index) instanceof Double);
        Assert.assertEquals(testData.get(key), t.getDouble(index));
        Assert.assertTrue(t.getDoubleByField(key) instanceof Double);
        Assert.assertEquals(testData.get(key), t.getDoubleByField(key));
    }

    /**
     * Test getting a float value.
     */
    @Test
    public void testGetFloat() {
        TestTuple t = new TestTuple(testData);
        String key = "float";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getFloat(index) instanceof Float);
        Assert.assertEquals(testData.get(key), t.getFloat(index));
        Assert.assertTrue(t.getFloatByField(key) instanceof Float);
        Assert.assertEquals(testData.get(key), t.getFloatByField(key));
    }

    /**
     * Test getting a binary value.
     */
    @Test
    public void testGetBinary() {
        TestTuple t = new TestTuple(testData);
        String key = "binary";

        int index = t.getFields().fieldIndex(key);
        Assert.assertTrue(t.getBinary(index) instanceof byte[]);
        Assert.assertEquals(testData.get(key), t.getBinary(index));
        Assert.assertTrue(t.getBinaryByField(key) instanceof byte[]);
        Assert.assertEquals(testData.get(key), t.getBinaryByField(key));
    }

    /**
     * Test get values.
     */
    @Test
    public void testGetValues() {
        TestTuple t = new TestTuple(testData);
        List<Object> values = t.getValues();

        for (Object o : values) {
            Assert.assertTrue(testData.values().contains(o));
        }
    }
}
