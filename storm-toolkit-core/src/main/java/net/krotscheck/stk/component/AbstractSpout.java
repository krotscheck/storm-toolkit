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

package net.krotscheck.stk.component;

import net.krotscheck.stk.stream.AbstractStreamEmitter;
import net.krotscheck.stk.stream.Schema;
import net.krotscheck.stk.stream.Stream;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.utils.Utils;

/**
 * This abstract implementation of the Spout interface adds strict, opinionated
 * schema handling, as well as convenience methods which manage the collector.
 * It also locks the cleanup() method, as its use is not guaranteed by the
 * topology.
 *
 * @author Michael Krotscheck
 */
public abstract class AbstractSpout
        extends AbstractStreamEmitter
        implements IRichSpout {

    /**
     * The collector provided to this spout.
     */
    private SpoutOutputCollector collector;

    /**
     * The storm configuration.
     */
    private Map stormConf;

    /**
     * The topology context object.
     */
    private TopologyContext topologyContext;

    /**
     * Generic invalidation method, can be invoked at any time when the
     * parameters in this spout change.
     */
    protected final void invalidate() {
        // Execute the spout configuration.
        configure(stormConf, topologyContext);

        // Invalidate the calculated, emitted streams.
        invalidateEmittedStreams();
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
     *                  information, etc.
     */
    protected abstract void configure(final Map stormConf,
                                      final TopologyContext context);

    /**
     * Emit a tuple to a given stream with a provided message ID.
     *
     * @param streamId  The ID of the stream to emit to.
     * @param tuple     The emitted tuple.
     * @param messageId The unique message ID used to track this message through
     *                  the topology.
     * @return The list of id's to which this tuple was emitted.
     */
    protected final List<Integer> emit(final String streamId,
                                       final Map<String, Object> tuple,
                                       final String messageId) {
        Stream s = getEmittedStream(streamId);
        if (s == null || collector == null) {
            return Collections.emptyList();
        }

        List<Object> outputTuple = castToStreamSchema(s, tuple);
        return collector.emit(streamId, outputTuple, messageId);
    }

    /**
     * Emit a tuple to the default stream, anchored to several different input
     * tuples.
     *
     * @param tuple     The emitted tuple.
     * @param messageId The unique message ID used to track this message through
     *                  the topology.
     * @return A list of message ID's.
     */
    protected final List<Integer> emit(final Map<String, Object> tuple,
                                       final String messageId) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }

    /**
     * This method takes a map of output values, and converts it into the sorted
     * list of tuple values that matches the stream definition.
     *
     * Storm assumes that an emitted tuple perfectly matches the declared field
     * order from declareOutputStream. This method ensures that that is true.
     *
     * @param stream The stream.
     * @param tuple  The map of tuple values to convert.
     * @return A sorted output tuple.
     */
    protected final List<Object> castToStreamSchema(
            final Stream stream, final Map<String, Object> tuple) {
        List<Object> outputTuple = new LinkedList<>();
        Schema schema = stream.getSchema();
        for (String key : schema.keySet()) {
            if (tuple.containsKey(key)) {
                outputTuple.add(tuple.get(key));
            } else {
                // All values must be present.
                outputTuple.add(null);
            }
        }
        return outputTuple;
    }

    /**
     * This method reports an error to the supervisor.
     *
     * @param throwable The error that occurred.
     */
    protected final void reportError(final Throwable throwable) {
        if (collector != null) {
            collector.reportError(throwable);
        }
    }

    /**
     * Called when a task for this component is initialized within a worker on
     * the cluster. It provides the spout with the environment in which the
     * spout executes.
     *
     * <p>This includes the:</p>
     *
     * @param stormConf The Storm configuration for this spout. This is the
     *                  configuration provided to the topology merged in with
     *                  cluster configuration on this machine.
     * @param context   This object can be used to get information about this
     *                  task's place within the topology, including the task id
     *                  and component id of this task, input and output
     *                  information, etc.
     * @param collector The collector is used to emit tuples from this spout.
     *                  Tuples can be emitted at any time, including the open
     *                  and close methods. The collector is thread-safe and
     *                  should be saved as an instance variable of this spout
     *                  object.
     */
    @Override
    public final void open(final Map stormConf,
                           final TopologyContext context,
                           final SpoutOutputCollector collector) {
        this.collector = collector;
        this.topologyContext = context;
        this.stormConf = stormConf;
        invalidate();
    }

    /**
     * Called when an ISpout is going to be shutdown. There is no guarentee that
     * close will be called, because the supervisor kill -9's worker processes
     * on the cluster.
     */
    @Override
    public final void close() {
        collector = null;
        topologyContext = null;
        stormConf = null;
        invalidate();
    }
}
