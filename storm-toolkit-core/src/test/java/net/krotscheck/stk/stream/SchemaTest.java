/*
 * Copyright (c) 2015 Michael Krotscheck
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

import net.krotscheck.stk.stream.Schema.Builder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

/**
 * Unit tests for the schema class. Assert that this is an unmodifiable map
 * which can only be created via the builder.
 *
 * @author Michael Krotscheck
 */
public final class SchemaTest {

    /**
     * Test raw schema.
     */
    private Map<String, Type> testRaw;

    /**
     * Test schema instance.
     */
    private Schema testSchema;

    /**
     * Test setup (mostly dummy data creation).
     *
     * @throws Exception Any unexpected exception.
     */
    @Before
    public void setUp() throws Exception {
        testRaw = new HashMap<>();
        testRaw.put("col_string", Type.STRING);
        testRaw.put("col_integer", Type.INT);
        testRaw.put("col_byte", Type.BYTE);

        testSchema = new Builder().add(testRaw).build();
    }

    /**
     * Test teardown.
     */
    @After
    public void tearDown() {
        testRaw = null;
        testSchema = null;
    }

    /**
     * Assert that the constructor is private.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test(expected = InstantiationException.class)
    public void testPrivateConstructor() throws Exception {
        Schema.class.newInstance(); // exception here
    }

    /**
     * Assert that a constructed schema does not support put().
     *
     * @throws Exception Any unexpected exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testCannotPut() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b.build();
        s.put("Test", Type.BOOLEAN);
    }

    /**
     * Assert that a constructed schema does not support putAll().
     *
     * @throws Exception Any unexpected exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testCannotPutAll() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b.build();
        s.putAll(testRaw);
    }

    /**
     * Assert that a constructed schema does not support clear().
     *
     * @throws Exception Any unexpected exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testCannotClear() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b.build();
        s.clear();
    }

    /**
     * Assert that a constructed schema does not support remove().
     *
     * @throws Exception Any unexpected exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testCannotReomve() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b.build();
        s.remove("Test");
    }

    /**
     * Assert that a constructed schema cannot be modified.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBasicMapFunctions() throws Exception {
        Builder b = new Schema.Builder();
        Schema e = b.build();

        // Test an empty schema.
        Assert.assertEquals("Size must be 0",
                0, e.size());

        Assert.assertTrue("Must not be empty",
                e.isEmpty());

        // Test a populated schema.
        Schema s = b.add(testRaw).build();

        Assert.assertEquals("Size must be " + testRaw.size(),
                testRaw.size(), s.size());

        Assert.assertFalse("Must not be empty",
                s.isEmpty());

        Assert.assertFalse("Must not be empty",
                s.isEmpty());

        Assert.assertTrue("Must contain 'col_string'",
                s.containsKey("col_string"));

        Assert.assertFalse("Must not contain 'col_foo'",
                s.containsKey("col_foo"));

        Assert.assertTrue("Must contain STRING",
                s.containsValue(Type.STRING));

        Assert.assertFalse("Must not contain DOUBLE",
                s.containsValue(Type.DOUBLE));

        Assert.assertEquals("Must not contain STRING at 'col_string'",
                Type.STRING, s.get("col_string"));

        Set<String> keySet = s.keySet();
        Assert.assertEquals("Must provide keyset",
                testRaw.size(), keySet.size());

        Collection<Type> values = s.values();
        Assert.assertEquals("Must provide values",
                testRaw.size(), values.size());

        Set<Entry<String, Type>> entries = s.entrySet();
        Assert.assertEquals("Must provide entries",
                testRaw.size(), entries.size());

    }

    /**
     * Test that the builder subclass generates an expected schema.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilder() throws Exception {
        Builder b = new Schema.Builder();
        Object s = b.add(testRaw).build();

        Assert.assertTrue("Output must be schema",
                s instanceof Schema);
    }

    /**
     * Test that the builder subclass generates distinct schema instances with
     * multiple invocations.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilderGeneratesDistinct() throws Exception {
        Builder b = new Schema.Builder();
        Schema s1 = b.add(testRaw).build();
        Schema s2 = b.build();

        Assert.assertNotSame(s1, s2);
    }

    /**
     * Test that the builder subclass can have schema added.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilderAdd() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b
                .add("column_test_1", Type.BOOLEAN)
                .add("column_test_2", Type.INT)
                .build();

        Assert.assertEquals("Size must be 2",
                2, s.size());

        Assert.assertEquals("Value must be BOOLEAN",
                Type.BOOLEAN, s.get("column_test_1"));
        Assert.assertEquals("Value must be INT",
                Type.INT, s.get("column_test_2"));
    }

    /**
     * Test that the builder merges identical columns.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilderAddMerge() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b
                .add("column_test_1", Type.BOOLEAN)
                .add("column_test_1", Type.BOOLEAN)
                .build();

        Assert.assertEquals("Size must be 1",
                1, s.size());

        Assert.assertEquals("Value must be BOOLEAN",
                Type.BOOLEAN, s.get("column_test_1"));
    }

    /**
     * Test that the builder can add batch schema.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilderAddBatch() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b.add(testRaw).build();

        Assert.assertEquals("Size must be " + testRaw.size(),
                testRaw.size(), s.size());

        for (String key : testRaw.keySet()) {
            Type expected = testRaw.get(key);
            Type actual = s.get(key);
            Assert.assertEquals("Type must match", expected, actual);
        }
    }

    /**
     * Test that the builder can add a schema.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilderAddSchema() throws Exception {
        Builder b = new Schema.Builder();
        Schema s = b.add(testSchema).build();

        Assert.assertEquals("Size must be " + testSchema.size(),
                testSchema.size(), s.size());

        for (String key : testSchema.keySet()) {
            Type expected = testSchema.get(key);
            Type actual = s.get(key);
            Assert.assertEquals("Type must match", expected, actual);
        }
    }

    /**
     * Test that conflicting column schemae are overwritten.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testConflictingColumns() throws Exception {
        Builder b = new Builder();
        b.add("test", Type.BOOLEAN);
        b.add("test", Type.INT);

        Schema s = b.build();
        Assert.assertEquals(1, s.size());
        Assert.assertEquals(Type.INT, s.get("test"));
    }

    /**
     * Test that streams' equality can be tested.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testEquals() throws Exception {
        Builder b = new Builder().add(testSchema);
        Builder bf = new Builder();
        Schema s1 = b.build();
        Schema s2 = b.build();
        Schema s3 = bf.build();

        Assert.assertEquals(s1, s1);
        Assert.assertEquals(s1, s2);
        Assert.assertEquals(s2, s1);
        Assert.assertNotEquals(s1, s3);
        Assert.assertNotEquals(s2, s3);
        Assert.assertNotEquals(s3, s1);
        Assert.assertNotEquals(s3, s2);

        Assert.assertNotEquals(new Object(), s1);
        Assert.assertNotEquals(s1, new Object());

        // Null checks
        Assert.assertFalse(s1.equals(null));
    }

    /**
     * Test that similar (cloned) versions of a schema are detected in a hash.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testHash() throws Exception {
        Builder b = new Builder().add(testSchema);
        Schema s1 = b.build();
        Schema s2 = b.build();

        HashSet<Schema> hs = new HashSet<>();
        hs.add(s1);

        Assert.assertTrue(hs.contains(s2));
    }

    /**
     * Test that the comparator is not set.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testComparator() throws Exception {
        Builder b = new Builder().add(testSchema);
        Schema s1 = b.build();
        Comparator<? super String> c = s1.comparator();
        Assert.assertNull(c);
    }

    /**
     * Test that the schema can be serialized.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testSerializable() throws Exception {
        Builder b = new Builder().add(testSchema);
        Schema s = b.build();

        // Serialize the object
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(s);
        byte[] result = baos.toByteArray();
        oos.close();

        // Deserialize the object
        ByteArrayInputStream bais = new ByteArrayInputStream(result);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object deserializedSchema = ois.readObject();

        Assert.assertTrue(s.equals(deserializedSchema));
    }

    /**
     * Test that we can extract subschemae.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testSubMap() throws Exception {
        Builder b = new Builder()
                .add("a", Type.BOOLEAN)
                .add("c", Type.BOOLEAN)
                .add("d", Type.BOOLEAN)
                .add("e", Type.BOOLEAN)
                .add("b", Type.BOOLEAN);
        Schema s = b.build();
        SortedMap<String, Type> subMap = s.subMap("b", "d");
        Assert.assertEquals(2, subMap.size());
        Assert.assertNotNull(subMap.get("b"));
        Assert.assertNotNull(subMap.get("c"));
    }

    /**
     * Test that we can extract a headmap.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testHeadMap() throws Exception {
        Builder b = new Builder()
                .add("a", Type.BOOLEAN)
                .add("c", Type.BOOLEAN)
                .add("d", Type.BOOLEAN)
                .add("e", Type.BOOLEAN)
                .add("b", Type.BOOLEAN);
        Schema s = b.build();
        SortedMap<String, Type> headMap = s.headMap("c");
        Assert.assertEquals(2, headMap.size());
        Assert.assertNotNull(headMap.get("a"));
        Assert.assertNotNull(headMap.get("b"));
    }

    /**
     * Test that we can extract a tailmap.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testTailMap() throws Exception {
        Builder b = new Builder()
                .add("a", Type.BOOLEAN)
                .add("c", Type.BOOLEAN)
                .add("d", Type.BOOLEAN)
                .add("e", Type.BOOLEAN)
                .add("b", Type.BOOLEAN);
        Schema s = b.build();
        SortedMap<String, Type> tailMap = s.tailMap("d");
        Assert.assertEquals(2, tailMap.size());
        Assert.assertNotNull(tailMap.get("d"));
        Assert.assertNotNull(tailMap.get("e"));
    }

    /**
     * Test that we can retrieve the first and last key.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testFirstLastKey() throws Exception {
        Builder b = new Builder()
                .add("a", Type.BOOLEAN)
                .add("c", Type.BOOLEAN)
                .add("d", Type.BOOLEAN)
                .add("e", Type.BOOLEAN)
                .add("b", Type.BOOLEAN);
        Schema s = b.build();
        Assert.assertEquals("a", s.firstKey());
        Assert.assertEquals("e", s.lastKey());
    }
}
