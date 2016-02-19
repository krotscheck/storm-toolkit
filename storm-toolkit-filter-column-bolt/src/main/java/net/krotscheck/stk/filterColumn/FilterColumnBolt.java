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

package net.krotscheck.stk.filterColumn;

import net.krotscheck.stk.component.AbstractSingleTupleBolt;
import net.krotscheck.stk.component.exception.BoltProcessingException;
import net.krotscheck.stk.stream.Stream;
import net.krotscheck.stk.stream.Type;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.shade.org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * This bolt will remove add or remove columns from a data stream, based on the
 * requested output schema.
 *
 * @author Michael Krotscheck
 */
public final class FilterColumnBolt
        extends AbstractSingleTupleBolt
        implements Serializable {

    /**
     * Retrieve the set of requested columns.
     *
     * @return An unmodifiable set of requested column names.
     */
    public Set<String> getRequestedColumns() {
        return Collections.unmodifiableSet(requestedColumns);
    }

    /**
     * Set the set of requested columns.
     *
     * @param requestedColumns The set of requested columns.
     */
    public void setRequestedColumns(final Set<String> requestedColumns) {
        this.requestedColumns = new TreeSet<>(requestedColumns);
    }

    /**
     * The set of columns that should be emitted from this bolt.
     */
    private SortedSet<String> requestedColumns = new TreeSet<String>();

    /**
     * Process a tick event.
     *
     * @param tuple The tick tuple.
     */
    @Override
    protected void tick(final Tuple tuple) {
        // This bolt does nothing special on a tick event.
    }

    /**
     * Process a single tuple.
     *
     * @param input The tuple to process.
     * @throws BoltProcessingException An exception encountered during bolt
     *                                 processing. This will trigger a
     *                                 reportError() and a fail() to be sent to
     *                                 the outputCollector.
     */
    @Override
    protected void process(final Tuple input) throws BoltProcessingException {
        Map<String, Object> outputTuple = requestedColumns.stream()
                .map(p -> Maps.immutableEntry(p,
                        input.contains(p) ? input.getValueByField(p) : null))
                .collect(TreeMap::new,
                        (m, e) -> m.put(e.getKey(), e.getValue()),
                        TreeMap::putAll);
        emit(input.getSourceStreamId(), input, outputTuple);
    }

    /**
     * Test to see whether two different filter column bolts perform the same
     * operation.
     *
     * @param o The other object to test
     * @return True if the instances perform the same operation, else false.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FilterColumnBolt that = (FilterColumnBolt) o;

        return new EqualsBuilder()
                .append(getRequestedColumns(), that.getRequestedColumns())
                .isEquals();
    }

    /**
     * Generate a unique hashcode for this instance.
     *
     * @return A hashcode.
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getRequestedColumns())
                .toHashCode();
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
        // No additional configuration based on the config or the context is
        // necessary.
    }

    /**
     * Whenever the provided streams are changed, this method is invoked to
     * trigger the component to recalculate the emitted streams.
     *
     * @param providedStreams The number of streams provided to this component.
     * @return A set of emitted streams.
     */
    @Override
    protected Collection<Stream> calculateEmittedStreams(
            final Collection<Stream> providedStreams) {
        List<Stream> emitted = new ArrayList<>();
        for (Stream provided : providedStreams) {
            Stream.Builder streamBuilder =
                    new Stream.Builder(provided.getStreamId());

            // Filter out whatever we don't have.
            Map<String, Type> filtered = provided.getSchema().entrySet()
                    .stream()
                    .filter(p -> requestedColumns.contains(p.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            // Insert everything we don't have...
            for (String key : requestedColumns) {
                if (!filtered.containsKey(key)) {
                    filtered.put(key, Type.STRING);
                }
            }
            streamBuilder.addSchemaFields(filtered);
            emitted.add(streamBuilder.build());
        }
        return Collections.unmodifiableCollection(emitted);
    }

    /**
     * Declare configuration specific to this component.
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Collections.emptyMap();
    }
}
