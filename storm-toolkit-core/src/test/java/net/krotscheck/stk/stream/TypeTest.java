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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the type enum and its utility methods.
 */
public final class TypeTest {

    /**
     * Assert that there are 10 raw types.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testExpectedTypes() throws Exception {
        Assert.assertEquals("Expect 10 types",
                10, Type.values().length);

        // These should throw compilation exceptions, so we're including
        // them here as a precompile sanity check.
        Assert.assertNotNull(Type.BOOLEAN);
        Assert.assertNotNull(Type.BYTE);
        Assert.assertNotNull(Type.CHAR);
        Assert.assertNotNull(Type.DOUBLE);
        Assert.assertNotNull(Type.FLOAT);
        Assert.assertNotNull(Type.INT);
        Assert.assertNotNull(Type.LONG);
        Assert.assertNotNull(Type.SHORT);
        Assert.assertNotNull(Type.STRING);
        Assert.assertNotNull(Type.NULL);
    }

    /**
     * Assert boolean type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testBooleanTypeDetection() throws Exception {

        Assert.assertEquals("Test Boolean detection",
                Type.BOOLEAN, Type.getTypeForObject(true));
        Assert.assertEquals("Test Boolean detection",
                Type.BOOLEAN, Type.getTypeForObject(Boolean.FALSE));
    }

    /**
     * Assert Byte type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testByteTypeDetection() throws Exception {

        Assert.assertEquals("Test Byte detection",
                Type.BYTE, Type.getTypeForObject((byte) 1));
        Assert.assertEquals("Test Byte detection",
                Type.BYTE, Type.getTypeForObject(Byte.valueOf("1")));
    }

    /**
     * Assert Char type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testCharTypeDetection() throws Exception {

        Assert.assertEquals("Test Char detection",
                Type.CHAR, Type.getTypeForObject('c'));
        Assert.assertEquals("Test Char detection",
                Type.CHAR, Type.getTypeForObject(Character.valueOf('c')));
    }

    /**
     * Assert Double type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testDoubleTypeDetection() throws Exception {
        double d = 1.2;
        Assert.assertEquals("Test Double detection",
                Type.DOUBLE, Type.getTypeForObject(d));
        Assert.assertEquals("Test Double detection",
                Type.DOUBLE, Type.getTypeForObject(Double.valueOf(d)));
    }

    /**
     * Assert Float type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testFloatTypeDetection() throws Exception {
        float f = (float) 1.2;
        Assert.assertEquals("Test Float detection",
                Type.FLOAT, Type.getTypeForObject(f));
        Assert.assertEquals("Test Float detection",
                Type.FLOAT, Type.getTypeForObject(Float.valueOf(f)));
    }

    /**
     * Assert Int type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testIntTypeDetection() throws Exception {
        int i = 1;
        Assert.assertEquals("Test Integer detection",
                Type.INT, Type.getTypeForObject(i));
        Assert.assertEquals("Test Integer detection",
                Type.INT, Type.getTypeForObject(Integer.valueOf(i)));
    }

    /**
     * Assert Long type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testLongTypeDetection() throws Exception {
        long l = 1;
        Assert.assertEquals("Test Long detection",
                Type.LONG, Type.getTypeForObject(l));
        Assert.assertEquals("Test Long detection",
                Type.LONG, Type.getTypeForObject(Long.valueOf(l)));
    }

    /**
     * Assert Short type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testShortTypeDetection() throws Exception {
        short s = 1;
        Assert.assertEquals("Test Short detection",
                Type.SHORT, Type.getTypeForObject(s));
        Assert.assertEquals("Test Short detection",
                Type.SHORT, Type.getTypeForObject(Short.valueOf(s)));
    }

    /**
     * Assert String type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testStringTypeDetection() throws Exception {
        Assert.assertEquals("Test String detection",
                Type.STRING, Type.getTypeForObject("string"));
        Assert.assertEquals("Test String detection",
                Type.STRING, Type.getTypeForObject(String.valueOf("string")));
    }

    /**
     * Assert Null type detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testNullTypeDetection() throws Exception {
        Assert.assertEquals("Test Null detection",
                Type.NULL, Type.getTypeForObject(null));
    }

    /**
     * Assert Object Detection.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testObjectTypeDetection() throws Exception {
        Assert.assertEquals("Test Object detection",
                Type.STRING, Type.getTypeForObject(new Object()));
    }

    /**
     * Assert string conversion.
     *
     * @throws Exception Any unexpected exception.
     */
    @Test
    public void testToString() throws Exception {
        Assert.assertEquals("boolean", Type.BOOLEAN.toString());
        Assert.assertEquals("byte", Type.BYTE.toString());
        Assert.assertEquals("char", Type.CHAR.toString());
        Assert.assertEquals("double", Type.DOUBLE.toString());
        Assert.assertEquals("float", Type.FLOAT.toString());
        Assert.assertEquals("int", Type.INT.toString());
        Assert.assertEquals("long", Type.LONG.toString());
        Assert.assertEquals("short", Type.SHORT.toString());
        Assert.assertEquals("string", Type.STRING.toString());
        Assert.assertEquals("null", Type.NULL.toString());
    }

}
