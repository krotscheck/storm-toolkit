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
 * This interface describes a class which emits data tuples. Based on (optional)
 * configuration, this component should be able to generate its own output
 * stream declarations.
 *
 * @author Michael Krotscheck
 */
public interface IStreamEmitter extends IComponent {

    /**
     * Return the list of streams which this emitter provides.
     *
     * @return The list of streams.
     */
    Collection<Stream> getEmittedStreams();

    /**
     * Return the stream definition for a specific stream, by id.
     *
     * @param streamId The ID for the stream.
     * @return The stream defenition.
     */
    Stream getEmittedStream(String streamId);

    /**
     * Does this emitter emit any streams?
     *
     * @return True if it emits a stream, otherwise false.
     */
    Boolean hasEmittedStreams();
}
