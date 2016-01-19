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

package net.krotscheck.stk.component;

import net.krotscheck.stk.component.exception.BoltProcessingException;

import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;

/**
 * This implementation is for bolts that process one single tuple at a time. It
 * handles receiving, ack()'ing, and fail()'ing tuples.
 *
 * @author Michael Krotscheck
 */
public abstract class AbstractSingleTupleBolt extends AbstractBolt {

    /**
     * A single tuple has been received and should be processed. This
     * implementation assumes that the tuple will be immediately processed, and
     * can be ack()'d or fail()'d immediately.
     *
     * @param input The input tuple to be processed.
     */
    @Override
    public final void execute(final Tuple input) {
        if (TupleUtils.isTick(input)) {
            tick(input);
            ack(input);
            return;
        }

        try {
            process(input);
            ack(input);
        } catch (BoltProcessingException e) {
            reportError(e);
            fail(input);
        }
    }


    /**
     * Process a tick event.
     *
     * @param tuple The tick tuple.
     */
    protected abstract void tick(Tuple tuple);

    /**
     * Process a single tuple.
     *
     * @param input The tuple to process.
     * @throws BoltProcessingException An exception encountered during bolt
     *                                 processing. This will trigger a
     *                                 reportError() and a fail() to be sent to
     *                                 the outputCollector.
     */
    protected abstract void process(Tuple input) throws BoltProcessingException;
}
