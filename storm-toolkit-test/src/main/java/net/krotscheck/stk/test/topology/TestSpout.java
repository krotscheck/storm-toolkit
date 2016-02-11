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

package net.krotscheck.stk.test.topology;

import net.krotscheck.stk.component.AbstractSpout;
import net.krotscheck.stk.stream.Stream;
import net.krotscheck.stk.stream.Stream.Builder;
import net.krotscheck.stk.stream.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MockedSources;
import backtype.storm.tuple.Values;

/**
 * A testing sput which feeds our topology a predefined list of values. In
 * reality, this is a mock spout that is never really used - its merely acts as
 * a data container, and provides those values to the TestJob implementation. It
 * could, feasibly, be used in a full integration test with vagrant VM's, once
 * such a harness exists.
 *
 * @author Michael Krotscheck
 */
public final class TestSpout extends AbstractSpout {

    /**
     * Our data iterator.
     */
    private final SortedMap<String, Iterator<SortedMap<String, Object>>>
            dataIterators = new TreeMap<>();

    /**
     * List of emitted messageId's.
     */
    private final List<String> messageIds = new LinkedList<>();

    /**
     * List of successful messageId's.
     */
    private final List<String> successMessageIds = new LinkedList<>();

    /**
     * List of failed messageId's.
     */
    private final List<String> failedMessageIds = new LinkedList<>();

    /**
     * Collection of input data.
     */
    private final SortedMap<String, List<SortedMap<String, Object>>>
            inputData = new TreeMap<>();

    /**
     * The ID string.
     */
    private final Integer id;

    /**
     * Create a new instance of this spout, with a given topology id.
     *
     * @param id The unique ID of this spout within the topology.
     */
    public TestSpout(final Integer id) {
        this.id = id;
    }

    /**
     * Retrieve the ID of the spout.
     *
     * @return The spout's ID.
     */
    public Integer getId() {
        return id;
    }

    /**
     * Add a list of testing data for a given stream id.
     *
     * @param streamId The ID of the stream.
     * @param data     The data to provide.
     */
    public void addInputData(final String streamId,
                             final List<SortedMap<String, Object>> data) {
        if (!inputData.containsKey(streamId)) {
            inputData.put(streamId, new LinkedList<>());
        }

        // Since the order of the values() within the data maps matters, we
        // have to regenerate the input data every time.
        List<SortedMap<String, Object>> scanData = new LinkedList<>();
        scanData.addAll(inputData.get(streamId));
        scanData.addAll(data);

        // Go through all our data and build a set of all column titles.
        SortedSet<String> columnNames = new TreeSet<>();
        for (SortedMap<String, Object> row : scanData) {
            columnNames.addAll(row.keySet());
        }

        // Clear the previous data, and add all rows, with all expected
        // columns, in a sorted manner.
        List<SortedMap<String, Object>> newData = new LinkedList<>();
        for (SortedMap<String, Object> row : scanData) {
            SortedMap<String, Object> newRow = new TreeMap<>();
            newRow.putAll(row);

            for (String column : columnNames) {
                if (!newRow.containsKey(column)) {
                    newRow.put(column, null);
                }
            }
            newData.add(newRow);
        }
        inputData.put(streamId, newData);
        invalidate();
    }

    /**
     * Add a single testing data record for a given stream id.
     *
     * @param streamId The ID of the stream.
     * @param data     The data to provide.
     */
    public void addInputData(final String streamId,
                             final SortedMap<String, Object> data) {
        List<SortedMap<String, Object>> dataList = new ArrayList<>();
        dataList.add(data);
        addInputData(streamId, dataList);
    }

    /**
     * Return the input data for a specific stream ID.
     *
     * @param streamId The stream ID.
     * @return The input data for this stream.
     */
    public List<SortedMap<String, Object>> getInputData(final String streamId) {
        return inputData.get(streamId);
    }

    /**
     * Declare configuration specific to this component.
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Collections.emptyMap();
    }

    /**
     * Called when a spout has been activated out of a deactivated mode.
     * nextTuple will be called on this spout soon. A spout can become activated
     * after having been deactivated when the topology is manipulated using the
     * `storm` client.
     */
    @Override
    public void activate() {
        dataIterators.clear();
        successMessageIds.clear();
        failedMessageIds.clear();
        messageIds.clear();

        // Generate our data iterators.
        for (String streamId : inputData.keySet()) {
            List<SortedMap<String, Object>> data = inputData.get(streamId);
            dataIterators.put(streamId, data.iterator());
        }
    }

    /**
     * Called when a spout has been deactivated. nextTuple will not be called
     * while a spout is deactivated. The spout may or may not be reactivated in
     * the future.
     */
    @Override
    public void deactivate() {
        dataIterators.clear();
        successMessageIds.clear();
        failedMessageIds.clear();
        messageIds.clear();
    }

    /**
     * Iterate over our data iterators.
     */
    @Override
    public void nextTuple() {
        // Get the next iterator.
        for (Entry<String, Iterator<SortedMap<String, Object>>> i
                : dataIterators.entrySet()) {
            if (i.getValue().hasNext()) {
                String messageId = UUID.randomUUID().toString();
                messageIds.add(messageId);
                emit(i.getKey(), i.getValue().next(), messageId);
                return;
            }
        }
    }

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId
     * identifier has been fully processed. Typically, an implementation of this
     * method will take that message off the queue and prevent it from being
     * replayed.
     */
    @Override
    public void ack(final Object msgId) {
        messageIds.remove(msgId.toString());
        successMessageIds.add(msgId.toString());
    }

    /**
     * The tuple emitted by this spout with the msgId identifier has failed to
     * be fully processed. Typically, an implementation of this method will put
     * that message back on the queue to be replayed at a later time.
     */
    @Override
    public void fail(final Object msgId) {
        messageIds.remove(msgId.toString());
        failedMessageIds.add(msgId.toString());
    }

    /**
     * Return the list of successful message ID's.
     *
     * @return The list of successful message id's.
     */
    public List<String> getSuccessMessageIds() {
        return Collections.unmodifiableList(successMessageIds);
    }

    /**
     * Return the list of failed message ID's.
     *
     * @return The list of failed message id's.
     */
    public List<String> getFailedMessageIds() {
        return Collections.unmodifiableList(failedMessageIds);
    }

    /**
     * Return the list of outstanding message ID's.
     *
     * @return The list of unprocessed message id's.
     */
    public List<String> getMessageIds() {
        return Collections.unmodifiableList(messageIds);
    }

    /**
     * Apply this spouts streams and values to a mocked source configuration,
     * used in a testing topology.
     *
     * @param sources The sources instance to configure.
     */
    public void apply(final MockedSources sources) {
        for (String streamId : inputData.keySet()) {

            List<SortedMap<String, Object>> data = inputData.get(streamId);
            List<Values> values = new ArrayList<>();

            for (SortedMap<String, Object> row : data) {
                Values v = new Values();
                v.addAll(row.values());
                values.add(v);
            }

            sources.addMockData(getId().toString(),
                    streamId,
                    values.toArray(new Values[values.size()]));
        }
    }

    /**
     * Update this spout's configuration based on the storm configuration and
     * the topology context.
     *
     * @param stormConf The Storm configuration for this spout. This is the
     *                  configuration provided to the topology merged in with
     *                  cluster configuration on this machine.
     * @param context   This object can be used to get information about this
     *                  task's place within the topology, including the task id
     *                  and component id of this task, input and output
     */
    @Override
    protected void configure(final Map stormConf,
                             final TopologyContext context) {
        // Do nothing
    }

    /**
     * Whenever the provided streams are changed, this method is invoked to
     * trigger the component to recalculate the emitted streams.
     *
     * @return A list of emitted streams.
     */
    @Override
    protected Collection<Stream> calculateEmittedStreams() {
        List<Stream> calculatedStreams = new LinkedList<>();
        for (String streamId : inputData.keySet()) {
            Builder b = new Stream.Builder(streamId);
            // Try to evaluate the schema from the first record.
            List<SortedMap<String, Object>> data = inputData.get(streamId);
            SortedMap<String, Object> row = data.get(0);
            for (String key : row.keySet()) {
                b.addSchemaField(key, Type.getTypeForObject(row.get(key)));
            }
            calculatedStreams.add(b.build());
        }
        return calculatedStreams;
    }
}
