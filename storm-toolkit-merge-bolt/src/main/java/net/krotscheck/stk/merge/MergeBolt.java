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

package net.krotscheck.stk.merge;

import net.krotscheck.stk.component.AbstractSingleTupleBolt;
import net.krotscheck.stk.component.exception.BoltProcessingException;
import net.krotscheck.stk.stream.Schema;
import net.krotscheck.stk.stream.Stream;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * The merge bolt allows us to combine two data streams. It doesn't attempt to
 * make intelligent guesses about what field needs to be added to which outgoing
 * tuple, it merely casts incoming tuples to the merged schema and sends them on
 * their way.
 *
 * @author Michael Krotscheck
 */
public final class MergeBolt extends AbstractSingleTupleBolt {

    /**
     * Process a tick event.
     *
     * @param tuple The tick tuple.
     */
    @Override
    protected void tick(final Tuple tuple) {
        // This bolt does nothing on the tick event.
    }

    /**
     * Process a single tuple, by casting it to the output schema and filling in
     * any nonexistent columns. WHile the STK framework does this inherently, we
     * nevertheless re-implement it here, as we cannot guarantee that the parent
     * framework will support this in the future.
     *
     * @param input The tuple to process. It will be ack'd automatically.
     * @throws BoltProcessingException An exception encountered during bolt
     *                                 processing. This will trigger a
     *                                 reportError() and a fail() to be sent to
     *                                 the outputCollector.
     */
    @Override
    protected void process(final Tuple input) throws BoltProcessingException {
        String streamId = input.getSourceStreamId();
        Stream emitted = getEmittedStream(streamId);
        if (emitted == null) {
            return;
        }
        Schema s = emitted.getSchema();

        // Extract the values into a map.
        Map<String, Object> outputMap = new HashMap<>();
        for (String fieldName : input.getFields()) {
            if (s.containsKey(fieldName)) {
                outputMap.put(fieldName, input.getValueByField(fieldName));
            }
        }

        // Add any columns required by the output schema.
        for (String outputField : emitted.getSchema().keySet()) {
            if (!outputMap.containsKey(outputField)) {
                outputMap.put(outputField, null);
            }
        }

        // Emit the tuple and exit.
        emit(streamId, input, outputMap);
    }

    /**
     * Update this bolt's configuration based on the storm configuration and the
     * topology context.
     *
     * @param stormConf The Storm configuration for this bolt. This is the
     *                  configuration provided to the topology merged in with
     *                  cluster configuration on this machine.
     * @param context   This object can be used to get information about this
     *                  task's place within the topology, including the task id
     *                  and component id of this task, input and output
     */
    @Override
    protected void configure(final Map stormConf,
                             final TopologyContext context) {
        // This bolt requires no real understanding of its context.
    }

    /**
     * Whenever the provided streams are changed, this method is invoked to
     * trigger the component to recalculate the emitted streams.
     *
     * @param providedStreams The number of streams provided to this component.
     * @return A list of emitted streams.
     */
    @Override
    protected Collection<Stream> calculateEmittedStreams(
            final Collection<Stream> providedStreams) {
        return Collections.unmodifiableCollection(providedStreams);
    }

    /**
     * Declare configuration specific to this component.
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Collections.emptyMap();
    }

    /**
     * Determine if this bolt is equal to another bolt.
     */
    @Override
    public boolean equals(final Object obj) {
        // Test for null and type mismatch.
        if (obj == null || !(obj instanceof MergeBolt)) {
            return false;
        }

        // Exit on strict equality
        if (this == obj) {
            return true;
        }

        // Equality is calculated from the output.
        MergeBolt that = (MergeBolt) obj;
        return this.getEmittedStreams()
                .containsAll(that.getEmittedStreams())
                && that.getEmittedStreams()
                .containsAll(this.getEmittedStreams());
    }

    /**
     * Generate a hashcode.
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (Stream s : getEmittedStreams()) {
            hashCode = 31 * hashCode + s.hashCode();
        }
        return hashCode;
    }
}
