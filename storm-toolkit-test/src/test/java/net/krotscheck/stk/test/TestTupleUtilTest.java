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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the TestTupleUtil.
 */
public final class TestTupleUtilTest {

    /**
     * Assert that the constructor is private.
     *
     * @throws Exception An unexpected exception.
     */
    @Test
    public void assertUtilityClass() throws Exception {
        Constructor<TestTupleUtil> constructor =
                TestTupleUtil.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
    }

    /**
     * Assert that the tuple util generates a tick tuple.
     */
    @Test
    public void testTickTuple() {
        Tuple t = TestTupleUtil.tickTuple();

        Assert.assertEquals(Constants.SYSTEM_COMPONENT_ID,
                t.getSourceComponent());
        Assert.assertEquals(Constants.SYSTEM_TICK_STREAM_ID,
                t.getSourceStreamId());
    }
}
