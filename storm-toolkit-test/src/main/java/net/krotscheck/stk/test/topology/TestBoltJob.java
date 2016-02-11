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

import net.krotscheck.stk.component.AbstractBolt;
import net.krotscheck.stk.stream.Stream;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This test job allows us to easily test a single bolt with some provided input
 * criteria.
 *
 * @author Michael Krotscheck
 */
public final class TestBoltJob implements TestJob {

    /**
     * A list of all input data spouts needed to test.
     */
    private final List<TestSpout> inputSpouts = new LinkedList<>();

    /**
     * The bolt under test.
     */
    private final AbstractBolt bolt;

    /**
     * The topology results, ordered by stream.
     */
    private final Map<String, List<SortedMap<String, Object>>> results
            = new TreeMap<>();

    /**
     * Add a stream of input data for this bolt.
     *
     * @param streamId The ID of the incoming stream.
     * @param data     The data which should be used.
     */
    public void addInputData(final String streamId,
                             final List<SortedMap<String, Object>> data) {
        TestSpout s = new TestSpout(inputSpouts.size());
        s.addInputData(streamId, data);
        inputSpouts.add(s);
    }

    /**
     * Create a new topology testing job for the provided bolt.
     *
     * @param bolt The bolt under test. This instance should be fully
     *             configured.
     */
    public TestBoltJob(final AbstractBolt bolt) {
        this.bolt = bolt;
    }

    /**
     * Return the processed results from this topology.
     *
     * @return The results, organized by streamId.
     */
    public Map<String, List<SortedMap<String, Object>>> getResults() {
        return results;
    }

    /**
     * Return the processed results for a specific stream.
     *
     * @param streamId The ID of the stream output to retrieve.
     * @return The results, organized by streamId.
     */
    public List<SortedMap<String, Object>> getResults(final String streamId) {
        return results.get(streamId);
    }

    /**
     * Run the testing logic with the cluster.
     *
     * @param cluster The local cluster.
     */
    @Override
    public void run(final ILocalCluster cluster) throws Exception {

        MockedSources ms = new MockedSources();
        TopologyBuilder builder = new TopologyBuilder();

        // Clean and add the bolt.
        bolt.clearProvidedStreams();
        Integer boltId = inputSpouts.size();
        BoltDeclarer bdc = builder.setBolt(boltId.toString(), bolt);

        for (TestSpout spout : inputSpouts) {
            // Add the spout
            builder.setSpout(spout.getId().toString(), spout);

            // Add the mock data for this spout
            spout.apply(ms);

            // Link the bolt-under-test to this spout.
            for (Stream emitted : spout.getEmittedStreams()) {
                bdc.globalGrouping(spout.getId().toString(),
                        emitted.getStreamId());
            }

            bolt.addProvidedStream(spout);
        }

        // Build the topology.
        StormTopology topology = builder.createTopology();
        Config conf = new Config();
        conf.setNumWorkers(1 + inputSpouts.size());

        CompleteTopologyParam cp = new CompleteTopologyParam();
        cp.setMockedSources(ms);
        cp.setTimeoutMs(50000);
        cp.setTopologyName("test");
        cp.setStormConf(conf);
        Map result = Testing.completeTopology(cluster, topology, cp);

        // Collect the results
        for (Stream stream : bolt.getEmittedStreams()) {
            List<List<Object>> tuples =
                    Testing.readTuples(result, boltId.toString(),
                            stream.getStreamId());

            List<SortedMap<String, Object>> streamResults = new ArrayList<>();
            // Cast the output to a map.
            for (List<Object> tuple : tuples) {
                SortedMap<String, Object> tupleMap = new TreeMap<>();
                Fields f = stream.getFields();
                for (int i = 0; i < f.size(); i++) {
                    String field = f.get(i);
                    tupleMap.put(field, tuple.get(i));
                }
                streamResults.add(tupleMap);
            }

            results.put(stream.getStreamId(), streamResults);
        }
    }
}
