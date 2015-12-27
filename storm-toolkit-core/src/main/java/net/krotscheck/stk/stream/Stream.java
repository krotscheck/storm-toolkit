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

package net.krotscheck.stk.stream;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Fields;

/**
 * Stream data type. Describes a named stream and the schema of the data tuples
 * in it.
 *
 * @author Michael Krotscheck
 */
public final class Stream implements Serializable {

    /**
     * Whether this is a direct stream or not.
     */
    private final Boolean direct;

    /**
     * The stream id.
     */
    private final String streamId;

    /**
     * The schema for this stream.
     */
    private final Schema schema;

    /**
     * This stream's schema, as a fields list.
     */
    private final Fields fields;

    /**
     * Constructor. Please use the builder.
     *
     * @param streamId The id for this stream.
     * @param direct   Whether this is a direct stream.
     * @param schema   The schema for this stream.
     */
    private Stream(final String streamId,
                   final Boolean direct,
                   final Schema schema) {
        this.streamId = streamId;
        this.direct = direct;
        this.schema = schema;

        List<String> fieldList = new LinkedList<>(); // Must be sorted.
        fieldList.addAll(getSchema().keySet()); // Schema is sorted.
        this.fields = new Fields(fieldList);
    }

    /**
     * Get the id for this stream.
     *
     * @return This stream's id
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Get the schema of this stream.
     *
     * @return The merged data schema for this stream.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Get the fields of this streams' schema.
     *
     * @return The fields for this schema.
     */
    public Fields getFields() {
        return fields;
    }

    /**
     * Is this a direct stream or not?
     *
     * @return Whether this is a direct stream.
     */
    public Boolean isDirect() {
        return direct;
    }

    /**
     * Check for equality.
     *
     * @param o The object to compare.
     * @return True if the instances are identical, otherwise false.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Stream stream = (Stream) o;

        if (!direct.equals(stream.direct)) {
            return false;
        }
        if (!streamId.equals(stream.streamId)) {
            return false;
        }
        return schema.equals(stream.schema);
    }

    /**
     * Generate a hashcode for this class.
     *
     * @return The generated hashcode.
     */
    @Override
    public int hashCode() {
        int result = direct.hashCode();
        result = 31 * result + streamId.hashCode();
        result = 31 * result + getSchema().hashCode();
        return result;
    }

    /**
     * Merge the values of the passed stream into the invoked stream, and
     * returns a new stream. Values from the master are overwritten by the child
     * where in conflict.
     *
     * @param insert The stream being inserted.
     * @return A new stream, with the schemae merged.
     */
    public Stream merge(final Stream insert) {
        // Create a new, merged stream.
        Builder b = new Builder(insert.getStreamId())
                .isDirect(insert.isDirect())
                .addSchema(schema)
                .addSchema(insert.getSchema());
        return b.build();
    }

    /**
     * Stream builder class. Use this to create immutable stream descriptors.
     */
    public static final class Builder {

        /**
         * The id for this stream.
         */
        private final String streamId;

        /**
         * Whether this is a direct stream or not.
         */
        private Boolean direct = false;

        /**
         * The schema builders in use for this stream.
         */
        private final Schema.Builder schemaBuilder = new Schema.Builder();

        /**
         * Create a new builder.
         *
         * @param streamId The ID of the stream to build.
         */
        public Builder(final String streamId) {
            this.streamId = streamId;
        }

        /**
         * Add a column to the stream's schema.
         *
         * @param name The name of the column.
         * @param type The data type of the column.
         * @return This builder.
         */
        public Builder addSchemaField(final String name, final Type type) {
            schemaBuilder.add(name, type);
            return this;
        }

        /**
         * Add all elements of one schema to those produced by the builder. This
         * will merge the fields in this schema with those of any schema already
         * registered.
         *
         * @param schema The schema to add.
         * @return This builder.
         */

        public Builder addSchema(final Schema schema) {
            schemaBuilder.add(schema);
            return this;
        }

        /**
         * Add all column/type mappings to the builder.
         *
         * @param schema The schema map to add.
         * @return This builder.
         */

        public Builder addSchemaFields(final Map<String, Type> schema) {
            schemaBuilder.add(schema);
            return this;
        }

        /**
         * Set whether this is a direct stream or not.
         *
         * @param direct Whether this is a direct stream.
         * @return This builder.
         */
        public Builder isDirect(final Boolean direct) {
            this.direct = direct;
            return this;
        }

        /**
         * Build the stream descriptor.
         *
         * @return A constructed stream descriptor.
         */
        public Stream build() {
            return new Stream(streamId, direct, schemaBuilder.build());
        }
    }
}
