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

import net.krotscheck.stk.cobertura.IgnoreCoverage;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Generates tuples for testing.
 *
 * @author Michael Krotscheck
 */
public final class TestTupleUtil {

    /**
     * Private constructor.
     */
    @IgnoreCoverage
    private TestTupleUtil() {
    }

    /**
     * Build a tick tuple for testing.
     *
     * @return A mocked tick tuple.
     */
    public static Tuple tickTuple() {
        Tuple tickTuple = mock(Tuple.class);
        when(tickTuple.getSourceComponent())
                .thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tickTuple.getSourceStreamId())
                .thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        return tickTuple;
    }
}
