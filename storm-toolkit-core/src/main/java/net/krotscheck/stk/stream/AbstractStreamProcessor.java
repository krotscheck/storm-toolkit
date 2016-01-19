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

package net.krotscheck.stk.stream;

import net.krotscheck.stk.stream.Stream.Builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * This abstract class processes tuples from a stream and sends them on. It is
 * functionally equivalent to a Bolt, though it does not yet implement the Bolt
 * Interface.
 *
 * @author Michael Krotscheck
 */
public abstract class AbstractStreamProcessor
        implements IStreamConsumer, IStreamEmitter {

    /**
     * Map of provided streams.
     */
    private final Map<String, Stream> provided = new HashMap<>();

    /**
     * Map of emitted streams.
     */
    private final Map<String, Stream> emitted = new HashMap<>();

    /**
     * Retrieve the streams provided to this consumer. Note that multiple
     * incoming streams may have the same name.
     *
     * @return List of streams.
     */
    @Override
    public final Collection<Stream> getProvidedStreams() {
        return Collections.unmodifiableCollection(provided.values());
    }

    /**
     * Add several input streams that will be provided to this consumer.
     *
     * @param providedStreams A collection of streams.
     */
    @Override
    public final void addProvidedStream(
            final Collection<Stream> providedStreams) {
        providedStreams.forEach(this::mergeProvidedStream);
        invalidateEmittedStreams();
    }

    /**
     * Add a single provided stream that will be added to this consumer.
     *
     * @param stream The stream to add.
     */
    @Override
    public final void addProvidedStream(final Stream stream) {
        mergeProvidedStream(stream);
        invalidateEmittedStreams();
    }

    /**
     * Add a stream emitter as the provider for this consumer. This will add all
     * the emitted streams to the consumer.
     *
     * @param emitter The IStreamEmitter that is providing the streams.
     */
    @Override
    public final void addProvidedStream(final IStreamEmitter emitter) {
        addProvidedStream(emitter.getEmittedStreams());
    }

    /**
     * Merge a provided stream in with the existing provided stream Collection.
     *
     * @param stream The provided stream.
     */
    private void mergeProvidedStream(final Stream stream) {
        if (provided.containsKey(stream.getStreamId())) {
            Stream old = provided.get(stream.getStreamId());
            Stream merged = old.merge(stream);
            provided.put(stream.getStreamId(), merged);
        } else {
            provided.put(stream.getStreamId(), stream);
        }
    }

    /**
     * Whenever the provided streams are changed, this method is invoked to
     * trigger the component to recalculate the emitted streams.
     *
     * @param providedStreams The number of streams provided to this component.
     * @return A list of emitted streams.
     */
    protected abstract Collection<Stream> calculateEmittedStreams(
            Collection<Stream> providedStreams);

    /**
     * This method triggers a recalculation of the emitted streams. It can be
     * invoked at any time, but please do so cautiously as it may carry unknown
     * processing overhead.
     */
    protected final void invalidateEmittedStreams() {

        // Clear the emitted streams
        emitted.clear();

        // Ask the implementor to generate a list of emitted streams.
        Collection<Stream> calcEmitted =
                calculateEmittedStreams(getProvidedStreams());
        if (calcEmitted == null) {
            // NPE guard.
            calcEmitted = new ArrayList<>();
        }

        for (Stream emittedStream : calcEmitted) {
            String streamId = emittedStream.getStreamId();

            // There is an edge case here where calculateEmittedStreams
            // generates a list that contains a stream duplicated by name,
            // but with a conflicting schema. In this case, the first schema
            // wins.
            if (emitted.containsKey(streamId)) {
                Stream previous = emitted.get(streamId);

                // We have to rebuild the stream.
                Builder b = new Stream.Builder(streamId)
                        .isDirect(previous.isDirect());

                // Create a new map of types and put/override values.
                Map<String, Type> newSchema = new TreeMap<>();
                newSchema.putAll(emittedStream.getSchema());
                newSchema.putAll(previous.getSchema()); // Order matters here.

                b.addSchemaFields(newSchema);

                emitted.put(streamId, b.build());
            } else {
                emitted.put(streamId, emittedStream);
            }
        }
    }

    /**
     * Indicates whether this worker has input streams to work with.
     *
     * @return True if streams have been provided, otherwise false.
     */
    @Override
    public final Boolean hasProvidedStreams() {
        return provided.size() > 0;
    }

    /**
     * Clear all provided streams.
     */
    @Override
    public final void clearProvidedStreams() {
        provided.clear();
    }

    /**
     * Return the list of streams which this emitter provides.
     *
     * @return The list of streams.
     */
    @Override
    public final Collection<Stream> getEmittedStreams() {
        return Collections.unmodifiableCollection(emitted.values());
    }

    /**
     * Return the stream definition for a specific stream, by id.
     *
     * @param streamId The ID for the stream.
     * @return The stream defenition.
     */
    @Override
    public final Stream getEmittedStream(final String streamId) {
        return emitted.get(streamId);
    }

    /**
     * Does this emitter emit any streams?
     *
     * @return True if it emits a stream, otherwise false.
     */
    @Override
    public final Boolean hasEmittedStreams() {
        return emitted.size() > 0;
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields,
     *                 and whether or not each output stream is a direct stream
     */
    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
        for (Stream s : getEmittedStreams()) {
            declarer.declareStream(
                    s.getStreamId(),
                    s.isDirect(),
                    s.getFields());
        }
    }
}
