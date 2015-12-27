/*
 * Copyright (c) 2015 Michael Krotscheck
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
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

import java.util.Collection;

import backtype.storm.topology.IComponent;

/**
 * This interface describes a class which accepts and manipulates data tuples.
 *
 * @author Michael Krotscheck
 */
public interface IStreamConsumer extends IComponent {

    /**
     * Retrieve the streams provided to this consumer. Note that multiple
     * incoming streams may have the same name.
     *
     * @return List of streams.
     */
    Collection<Stream> getProvidedStreams();

    /**
     * Add several input streams that will be provided to this consumer.
     *
     * @param providedStreams A collection of streams.
     */
    void addProvidedStream(Collection<Stream> providedStreams);

    /**
     * Add a single provided stream that will be added to this consumer.
     *
     * @param stream The stream to add.
     */
    void addProvidedStream(Stream stream);

    /**
     * Add a stream emitter as the provider for this consumer. This will add all
     * the emitted streams to the consumer.
     *
     * @param emitter The IStreamEmitter that is providing the streams.
     */
    void addProvidedStream(IStreamEmitter emitter);

    /**
     * Indicates whether this worker has input streams to work with.
     *
     * @return True if streams have been provided, otherwise false.
     */
    Boolean hasProvidedStreams();

    /**
     * Clear all provided streams.
     */
    void clearProvidedStreams();

}
