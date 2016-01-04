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

import net.krotscheck.stk.stream.AbstractStreamProcessor;
import net.krotscheck.stk.stream.Schema;
import net.krotscheck.stk.stream.Stream;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * This abstract implementation of the Bolt interface adds strict, opinionated
 * schema handling, as well as convenience methods which manage the
 * outputCollector. It also locks the cleanup() method, as its use is not
 * guaranteed by the topology.
 *
 * @author Michael Krotscheck
 */
public abstract class AbstractBolt
        extends AbstractStreamProcessor
        implements IRichBolt {

    /**
     * The outputcollector provided to this bolt.
     */
    private OutputCollector outputCollector;

    /**
     * The storm configuration.
     */
    private Map stormConf;

    /**
     * The topology context object.
     */
    private TopologyContext topologyContext;

    /**
     * Called when a task for this component is initialized within a worker on
     * the cluster. It provides the bolt with the environment in which the bolt
     * executes.
     *
     * <p>This includes the:</p>
     *
     * @param stormConf The Storm configuration for this bolt. This is the
     *                  configuration provided to the topology merged in with
     *                  cluster configuration on this machine.
     * @param context   This object can be used to get information about this
     *                  task's place within the topology, including the task id
     *                  and component id of this task, input and output
     *                  information, etc.
     * @param collector The collector is used to emit tuples from this bolt.
     *                  Tuples can be emitted at any time, including the prepare
     *                  and cleanup methods. The collector is thread-safe and
     *                  should be saved as an instance variable of this bolt
     *                  object.
     */
    @Override
    public final void prepare(final Map stormConf,
                              final TopologyContext context,
                              final OutputCollector collector) {
        this.outputCollector = collector;
        this.topologyContext = context;
        this.stormConf = stormConf;
        invalidate();
    }

    /**
     * Generic invalidation method, can be invoked at any time when the
     * parameters in this bolt change.
     */
    protected final void invalidate() {
        // Execute the bolt configuration.
        configure(stormConf, topologyContext);

        // Invalidate the calculated, emitted streams.
        invalidateEmittedStreams();
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
     *                  information, etc.
     */
    protected abstract void configure(final Map stormConf,
                                      final TopologyContext context);

    /**
     * Ack() a list of tuples.
     *
     * @param tuples The tuples to ack.
     */
    protected final void ack(final List<Tuple> tuples) {
        tuples.forEach(this::ack);
    }

    /**
     * Ack() a tuple.
     *
     * @param tuple The tuple to ack.
     */
    protected final void ack(final Tuple tuple) {
        if (outputCollector != null) {
            outputCollector.ack(tuple);
        }
    }

    /**
     * Emit a tuple anchored to several different input tuples with a given
     * string ID.
     *
     * @param streamId The ID of the stream to emit to.
     * @param anchors  A list of anchors related to the emitted tuple.
     * @param tuple    The emitted tuple.
     * @return The list of message ID's
     */
    protected final List<Integer> emit(final String streamId,
                                       final Collection<Tuple> anchors,
                                       final Map<String, Object> tuple) {
        Stream s = getEmittedStream(streamId);
        if (s == null || outputCollector == null) {
            return Collections.emptyList();
        }

        List<Object> outputTuple = castToStreamSchema(s, tuple);
        return outputCollector.emit(streamId, anchors, outputTuple);
    }

    /**
     * Emit a single anchored tuple with a given string ID.
     *
     * @param streamId The ID of the stream to emit to.
     * @param anchor   The anchor to emit this tuple for.
     * @param tuple    The emitted tuple.
     * @return The list of message ID's
     */
    protected final List<Integer> emit(final String streamId,
                                       final Tuple anchor,
                                       final Map<String, Object> tuple) {
        Stream s = getEmittedStream(streamId);
        if (s == null || outputCollector == null) {
            return Collections.emptyList();
        }

        List<Object> outputTuple = castToStreamSchema(s, tuple);
        return outputCollector.emit(streamId, anchor, outputTuple);
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
     * Emit a tuple to the default stream, anchored to several different input
     * tuples.
     *
     * @param anchors A list of anchors related to the emitted tuple.
     * @param tuple   The emitted tuple.
     * @return A list of message ID's.
     */
    protected final List<Integer> emit(final Collection<Tuple> anchors,
                                       final Map<String, Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }

    /**
     * Emit a single anchored tuple to the default stream.
     *
     * @param anchor The anchor to emit this tuple for.
     * @param tuple  The emitted tuple.
     * @return A list of message ID's.
     */
    protected final List<Integer> emit(final Tuple anchor,
                                       final Map<String, Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, anchor, tuple);
    }

    /**
     * Fail a list of tuples.
     *
     * @param tuples The tuples to fail.
     */
    protected final void fail(final List<Tuple> tuples) {
        tuples.forEach(this::fail);
    }

    /**
     * Fail a tuple.
     *
     * @param tuple The tuple to fail.
     */
    protected final void fail(final Tuple tuple) {
        if (outputCollector != null) {
            outputCollector.fail(tuple);
        }
    }

    /**
     * This method reports an error to the supervisor.
     *
     * @param throwable The error that occurred.
     */
    protected final void reportError(final Throwable throwable) {
        if (outputCollector != null) {
            outputCollector.reportError(throwable);
        }
    }

    /**
     * Called when an IBolt is going to be shutdown. There is no guarentee that
     * cleanup will be called, because the supervisor kill -9's worker processes
     * on the cluster. As such, this method is locked from extension.
     *
     * <p>The one context where cleanup is guaranteed to be called is when a
     * topology is killed when running Storm in local mode.</p>
     */
    @Override
    public final void cleanup() {
        outputCollector = null;
        topologyContext = null;
        stormConf = null;
        invalidate();
    }
}
