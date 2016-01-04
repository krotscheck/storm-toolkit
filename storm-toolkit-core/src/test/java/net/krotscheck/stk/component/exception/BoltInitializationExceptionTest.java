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

package net.krotscheck.stk.component.exception;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public final class BoltInitializationExceptionTest {

    /**
     * Assert that the message is throwable.
     */
    @Test
    public void testIsThrowable() {
        BoltInitializationException e = new BoltInitializationException("test");
        Assert.assertTrue(e instanceof Throwable);
    }

    /**
     * Assert that the message is created as expected.
     */
    @Test
    public void testMessage() {
        BoltInitializationException e =
                new BoltInitializationException("message");

        Assert.assertTrue(e.getMessage().equals("message"));
    }
}
