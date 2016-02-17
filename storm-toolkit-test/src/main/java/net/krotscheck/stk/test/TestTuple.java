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

package net.krotscheck.stk.test;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * An implementation of the Tuple interface, for testing purposes. It sets
 * various sane defaults for all properties, permitting the overriding of most.
 *
 * @author Michael Krotscheck
 */
public final class TestTuple implements Tuple {

    /**
     * The backing data.
     */
    private final Fields fields;

    /**
     * The data values.
     */
    private final LinkedList<Object> values;

    /**
     * The global stream ID.
     */
    private GlobalStreamId globalStreamid =
            new GlobalStreamId("1", Utils.DEFAULT_STREAM_ID);

    /**
     * The ID of the component (bolt/spout) that emitted this tuple.
     */
    private String sourceComponent = "1";

    /**
     * The ID of the source task.
     */
    private int sourceTask = 0;

    /**
     * The source stream ID.
     */
    private String sourceStreamId = Utils.DEFAULT_STREAM_ID;

    /**
     * The message ID.
     */
    private MessageId messageId = MessageId.makeUnanchored();

    /**
     * Constructor.
     *
     * @param rawTuple Raw data to back this tuple.
     */
    public TestTuple(final SortedMap<String, Object> rawTuple) {
        this.fields = new Fields(new LinkedList<>(rawTuple.keySet()));
        this.values = new LinkedList<>(rawTuple.values());
    }

    /**
     * Returns the global stream id (component + stream) of this tuple.
     */
    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return globalStreamid;
    }

    /**
     * Change the source component of the string.
     *
     * @param sourceComponent The source component.
     */
    public void setSourceComponent(final String sourceComponent) {
        this.sourceComponent = sourceComponent;
        this.globalStreamid = new GlobalStreamId(sourceComponent,
                sourceStreamId);
    }

    /**
     * Gets the id of the component that created this tuple.
     */
    @Override
    public String getSourceComponent() {
        return sourceComponent;
    }

    /**
     * Set the source task.
     *
     * @param sourceTask The source task.
     */
    public void setSourceTask(final int sourceTask) {
        this.sourceTask = sourceTask;
    }

    /**
     * Gets the id of the task that created this tuple.
     */
    @Override
    public int getSourceTask() {
        return sourceTask;
    }

    /**
     * Set the source stream ID.
     *
     * @param sourceStreamId The source stream ID.
     */
    public void setSourceStreamId(final String sourceStreamId) {
        this.sourceStreamId = sourceStreamId;
        this.globalStreamid = new GlobalStreamId(sourceComponent,
                sourceStreamId);
    }

    /**
     * Gets the id of the stream that this tuple was emitted to.
     */
    @Override
    public String getSourceStreamId() {
        return sourceStreamId;
    }

    /**
     * Change the messageId.
     *
     * @param messageId The messageId to use.
     */
    public void setMessageId(final MessageId messageId) {
        this.messageId = messageId;
    }

    /**
     * Gets the message id that associated with this tuple.
     */
    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    /**
     * Returns the number of fields in this tuple.
     */
    @Override
    public int size() {
        return getFields().size();
    }

    /**
     * Returns true if this tuple contains the specified name of the field.
     */
    @Override
    public boolean contains(final String field) {
        return getFields().contains(field);
    }

    /**
     * Gets the names of the fields in this tuple.
     */
    @Override
    public Fields getFields() {
        return fields;
    }

    /**
     * Returns the position of the specified field in this tuple.
     */
    @Override
    public int fieldIndex(final String field) {
        return getFields().fieldIndex(field);
    }

    /**
     * Returns a subset of the tuple based on the fields selector.
     */
    @Override
    public List<Object> select(final Fields selector) {
        LinkedList<Object> subset = new LinkedList<>();
        for (String field : selector) {
            subset.add(values.get(fieldIndex(field)));
        }
        return subset;
    }

    /**
     * Gets the field at position i in the tuple. Returns object since tuples
     * are dynamically typed.
     */
    @Override
    public Object getValue(final int i) {
        return values.get(i);
    }

    /**
     * Returns the String at position i in the tuple. If that field is not a
     * String, you will get a runtime error.
     */
    @Override
    public String getString(final int i) {
        return (String) values.get(i);
    }

    /**
     * Returns the Integer at position i in the tuple. If that field is not an
     * Integer, you will get a runtime error.
     */
    @Override
    public Integer getInteger(final int i) {
        return (Integer) values.get(i);
    }

    /**
     * Returns the Long at position i in the tuple. If that field is not a Long,
     * you will get a runtime error.
     */
    @Override
    public Long getLong(final int i) {
        return (Long) values.get(i);
    }

    /**
     * Returns the Boolean at position i in the tuple. If that field is not a
     * Boolean, you will get a runtime error.
     */
    @Override
    public Boolean getBoolean(final int i) {
        return (Boolean) values.get(i);
    }

    /**
     * Returns the Short at position i in the tuple. If that field is not a
     * Short, you will get a runtime error.
     */
    @Override
    public Short getShort(final int i) {
        return (Short) values.get(i);
    }

    /**
     * Returns the Byte at position i in the tuple. If that field is not a Byte,
     * you will get a runtime error.
     */
    @Override
    public Byte getByte(final int i) {
        return (Byte) values.get(i);
    }

    /**
     * Returns the Double at position i in the tuple. If that field is not a
     * Double, you will get a runtime error.
     */
    @Override
    public Double getDouble(final int i) {
        return (Double) values.get(i);
    }

    /**
     * Returns the Float at position i in the tuple. If that field is not a
     * Float, you will get a runtime error.
     */
    @Override
    public Float getFloat(final int i) {
        return (Float) values.get(i);
    }

    /**
     * Returns the byte array at position i in the tuple. If that field is not a
     * byte array, you will get a runtime error.
     */
    @Override
    public byte[] getBinary(final int i) {
        return (byte[]) values.get(i);
    }

    @Override
    public Object getValueByField(final String field) {
        return values.get(fieldIndex(field));
    }

    @Override
    public String getStringByField(final String field) {
        return (String) values.get(fieldIndex(field));
    }

    @Override
    public Integer getIntegerByField(final String field) {
        return (Integer) values.get(fieldIndex(field));
    }

    @Override
    public Long getLongByField(final String field) {
        return (Long) values.get(fieldIndex(field));
    }

    @Override
    public Boolean getBooleanByField(final String field) {
        return (Boolean) values.get(fieldIndex(field));
    }

    @Override
    public Short getShortByField(final String field) {
        return (Short) values.get(fieldIndex(field));
    }

    @Override
    public Byte getByteByField(final String field) {
        return (Byte) values.get(fieldIndex(field));
    }

    @Override
    public Double getDoubleByField(final String field) {
        return (Double) values.get(fieldIndex(field));
    }

    @Override
    public Float getFloatByField(final String field) {
        return (Float) values.get(fieldIndex(field));
    }

    @Override
    public byte[] getBinaryByField(final String field) {
        return (byte[]) values.get(fieldIndex(field));
    }

    /**
     * Gets all the values in this tuple.
     */
    @Override
    public List<Object> getValues() {
        return values;
    }
}
