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

import net.krotscheck.stk.stream.Stream.Builder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for the stream class. Assert that it has some methods, but is
 * mostly a passthrough to the underlying schema builder.
 *
 * @author Michael Krotscheck
 */
public final class StreamTest {

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

        testSchema = new Schema.Builder().add(testRaw).build();
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
        Stream.class.newInstance(); // exception here
    }

    /**
     * Assert that we can retrieve the name and the schema.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testGetBasicProperties() throws Exception {
        Stream s = new Builder("test")
                .addSchema(testSchema)
                .build();

        Assert.assertEquals("test", s.getStreamId());

        // Make sure the schema is cloned.
        Assert.assertNotSame(testSchema, s.getSchema());

        // Make sure all the field names match.
        Set<String> fieldSet = new HashSet<>();
        for (String field : s.getFields()) {
            fieldSet.add(field);
        }
        Assert.assertEquals(testSchema.keySet(), fieldSet);
    }

    /**
     * Assert that we can set the direct stream flag.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testSetDirect() throws Exception {
        Stream s = new Builder("test")
                .isDirect(true)
                .build();
        Assert.assertTrue(s.isDirect());
    }

    /**
     * Assert that we can add a schema.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddSchema() throws Exception {
        Stream s = new Builder("test")
                .addSchema(testSchema)
                .build();

        Schema schema = s.getSchema();
        Assert.assertEquals(testRaw.keySet(), schema.keySet());
    }

    /**
     * Assert that we can add a schema expressed as a map.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddSchemaFields() throws Exception {
        Stream s = new Builder("test")
                .addSchemaFields(testRaw)
                .build();

        Schema schema = s.getSchema();
        Assert.assertEquals(testRaw.keySet(), schema.keySet());
    }

    /**
     * Assert that we can add individual fields.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void testAddSchemaField() throws Exception {
        Stream s = new Builder("test")
                .addSchemaField("test", Type.BOOLEAN)
                .build();

        Schema schema = s.getSchema();
        Assert.assertTrue(schema.containsKey("test"));
        Assert.assertEquals(1, schema.size());
    }

    /**
     * Assert that adding multiple schemae merges them into one.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testMergeSchemae() throws Exception {
        Stream s = new Builder("test")
                .addSchema(testSchema)
                .addSchemaFields(testRaw)
                .build();

        // Make sure the schema is cloned.
        Assert.assertNotSame(testSchema, s.getSchema());

        // Make sure content is identical (since testRaw is the raw version
        // of testSchema).
        Assert.assertEquals(testSchema, s.getSchema());
    }

    /**
     * Test that the builder subclass generates an expected stream.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilder() throws Exception {
        Builder b = new Builder("test");
        Object s = b.addSchemaFields(testRaw).build();

        Assert.assertTrue("Output must be Stream",
                s instanceof Stream);
    }

    /**
     * Test that the builder subclass generates distinct stream instances with
     * multiple invocations.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBuilderGeneratesDistinct() throws Exception {
        Builder b = new Builder("test").addSchema(testSchema);
        Stream s1 = b.build();
        Stream s2 = b.build();

        Assert.assertNotSame(s1, s2);
        Assert.assertNotSame(s1.getSchema(), s2.getSchema());
    }

    /**
     * Test that we can merge two different streams.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testMergeBasic() throws Exception {
        Builder b1 = new Builder("default")
                .addSchemaField("field_1", Type.STRING)
                .addSchemaField("field_2", Type.BOOLEAN);
        Builder b2 = new Builder("default")
                .addSchemaField("field_3", Type.BYTE)
                .addSchemaField("field_4", Type.STRING);

        Stream s1 = b1.build();
        Stream s2 = b2.build();

        Stream sm = s1.merge(s2);
        Schema s = sm.getSchema();

        // Assert that two different schemae are merged.
        Assert.assertEquals(4, s.size());
        Assert.assertTrue(s.containsKey("field_1"));
        Assert.assertTrue(s.containsKey("field_2"));
        Assert.assertTrue(s.containsKey("field_3"));
        Assert.assertTrue(s.containsKey("field_4"));

        // Assert that the invoker's streamId is the same.
        Assert.assertEquals("default", sm.getStreamId());

        // Assert that the invoker's 'direct' flag is the same.
        Assert.assertFalse(sm.isDirect());
    }

    /**
     * Test that name differences throw conflicts.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testMergeNameConflict() throws Exception {
        Stream s1 = new Builder("default").build();
        Stream s2 = new Builder("not_default").build();

        Stream s3 = s1.merge(s2);
        Assert.assertNotSame(s3, s2);
        Assert.assertNotSame(s3, s1);
        // This check handles the equality, as we're not adding anything else
        // that could cause a hashCode conflict.
        Assert.assertEquals(s3, s2);
    }

    /**
     * Test that 'direct' differences throw conflicts.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testMergeDirectConflict() throws Exception {
        Stream s1 = new Builder("default").isDirect(true).build();
        Stream s2 = new Builder("default").isDirect(false).build();

        Stream s3 = s1.merge(s2);
        Assert.assertNotSame(s3, s2);
        Assert.assertNotSame(s3, s1);
        Assert.assertFalse(s3.isDirect());
    }

    /**
     * Test that streams' equality can be tested.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testEquals() throws Exception {
        Builder b = new Builder("default").addSchema(testSchema);
        Builder bm = new Builder("management").addSchema(testSchema);
        Builder be = new Builder("default");
        Builder bd = new Builder("default").isDirect(true);

        Stream s1 = b.build();
        Stream s2 = b.build();
        Stream s3 = be.build();
        Stream s4 = bm.build();
        Stream s5 = bd.build();

        Assert.assertEquals(s1, s1);
        Assert.assertEquals(s1, s2);
        Assert.assertEquals(s2, s1);
        Assert.assertNotEquals(s1, s3);
        Assert.assertNotEquals(s2, s3);
        Assert.assertNotEquals(s3, s1);
        Assert.assertNotEquals(s3, s2);

        Assert.assertNotEquals(s1, s4);
        Assert.assertNotEquals(s2, s4);
        Assert.assertNotEquals(s4, s1);
        Assert.assertNotEquals(s4, s2);

        Assert.assertNotEquals(s1, s5);
        Assert.assertNotEquals(s2, s5);
        Assert.assertNotEquals(s5, s1);
        Assert.assertNotEquals(s5, s2);

        Assert.assertNotEquals(new Object(), s1);
        Assert.assertNotEquals(s1, new Object());

        // Null test
        Assert.assertFalse(s1.equals(null));
    }

    /**
     * Test that different (cloned) versions of a schema are detected in a
     * hash.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testDiffHash() throws Exception {
        Stream s1 = new Stream.Builder("foo").addSchema(testSchema).build();
        Stream s2 = new Stream.Builder("foo1").addSchema(testSchema).build();
        Stream s3 = new Stream.Builder("foo").build();

        Assert.assertNotEquals(s1.hashCode(), s2.hashCode());
        Assert.assertNotEquals(s1.hashCode(), s3.hashCode());
    }

    /**
     * Test that similar (cloned) versions of a schema are detected in a hash.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testHash() throws Exception {
        Builder b = new Stream.Builder("foo").addSchema(testSchema);
        Stream s1 = b.build();
        Stream s2 = b.build();

        Assert.assertEquals(s1.hashCode(), s2.hashCode());

        HashSet<Stream> hs = new HashSet<>();
        hs.add(s1);

        Assert.assertTrue(hs.contains(s2));
    }

    /**
     * Test that the stream can be serialized.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testSerializable() throws Exception {
        Builder b = new Stream.Builder("foo").addSchema(testSchema);
        Stream s = b.build();

        // Serialize the object
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(s);
        byte[] result = baos.toByteArray();
        oos.close();

        // Deserialize the object
        ByteArrayInputStream bais = new ByteArrayInputStream(result);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object deserializedStream = ois.readObject();

        Assert.assertTrue(s.equals(deserializedStream));
    }
}
