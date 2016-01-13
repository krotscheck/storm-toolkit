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

import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the STKAssertion utility class.
 *
 * @author Michael Krotscheck
 */
public final class STKAssertTest {

    /**
     * Assert that the constructor is private.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void assertUtilityClass() throws Exception {
        Constructor<STKAssert> constructor =
                STKAssert.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
    }

    /**
     * Assert that a properly serializable object can be verified.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void assertValidOnReserialize() throws Exception {
        TestSerializable test = new TestSerializable();
        test.setFoo("test");
        STKAssert.assertReserializable(test);

    }

    /**
     * Assert that a properly unserializable object throws an exception.
     *
     * @throws Exception An unexpected exception.
     */
    @Test(expected = AssertionError.class)
    public void assertInvalidOnReserialize() throws Exception {
        TestNotSerializable test = new TestNotSerializable();
        test.setFoo("test");
        STKAssert.assertReserializable(test);
    }

    /**
     * A test class that doesn't implement equals and hashcode.
     */
    public static final class TestNotSerializable implements Serializable {

        /**
         * The container for foo.
         */
        private String foo;

        /**
         * Get foo.
         *
         * @return The value of foo.
         */
        public String getFoo() {
            return foo;
        }

        /**
         * Set foo.
         *
         * @param foo A new value for foo.
         */
        public void setFoo(final String foo) {
            this.foo = foo;
        }
    }

    /**
     * A test serializable class.
     */
    public static final class TestSerializable implements Serializable {

        /**
         * The container for foo.
         */
        private String foo;

        /**
         * Get foo.
         *
         * @return The value of foo.
         */
        public String getFoo() {
            return foo;
        }

        /**
         * Set foo.
         *
         * @param foo A new value for foo.
         */
        public void setFoo(final String foo) {
            this.foo = foo;
        }

        /**
         * Test to see if the items are equal.
         *
         * @param o The object to compare
         * @return True if they are the same, otherwise false.
         */
        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestSerializable that = (TestSerializable) o;

            if (getFoo() != null) {
                return getFoo().equals(that.getFoo());
            }
            return that.getFoo() == null;
        }

        /**
         * Generate a unique hashcode for this object.
         *
         * @return The hashcode.
         */
        @Override
        public int hashCode() {
            if (getFoo() != null) {
                return getFoo().hashCode();
            }
            return 0;
        }
    }
}
