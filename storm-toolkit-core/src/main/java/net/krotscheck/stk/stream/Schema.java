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

import org.apache.storm.shade.org.apache.commons.exec.util.MapUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Schema data type. Used to manage the data contract on input and output data
 * for worker streams. This type is immutable, please use the Builder class to
 * construct it.
 *
 * @author Michael Krotscheck
 */
public final class Schema
        implements SortedMap<String, Type>, Serializable {

    /**
     * Internal storage, should be immutable.
     */
    private final SortedMap<String, Type> internalSchema;

    /**
     * Please use the builder instead.
     *
     * @param schema The schema map to wrap.
     */
    private Schema(final Map<String, Type> schema) {
        this.internalSchema =
                Collections.unmodifiableSortedMap(new TreeMap<>(schema));
    }

    @Override
    public int size() {
        return internalSchema.size();
    }

    @Override
    public boolean isEmpty() {
        return internalSchema.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return internalSchema.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return internalSchema.containsValue(value);
    }

    @Override
    public Type get(final Object key) {
        return internalSchema.get(key);
    }

    @Override
    public Type put(final String key, final Type value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final Map<? extends String, ? extends Type> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the comparator used to order the keys in this map, or {@code
     * null} if this map uses the {@linkplain Comparable natural ordering} of
     * its keys.
     *
     * @return the comparator used to order the keys in this map, or {@code
     * null} if this map uses the natural ordering of its keys
     */
    @Override
    public Comparator<? super String> comparator() {
        return internalSchema.comparator();
    }

    /**
     * Returns a view of the portion of this map whose keys range from {@code
     * fromKey}, inclusive, to {@code toKey}, exclusive.  (If {@code fromKey}
     * and {@code toKey} are equal, the returned map is empty.)  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa. The returned map supports all optional map
     * operations that this map supports.
     *
     * <p>The returned map will throw an {@code IllegalArgumentException} on an
     * attempt to insert a key outside its range.
     *
     * @param fromKey low endpoint (inclusive) of the keys in the returned map
     * @param toKey   high endpoint (exclusive) of the keys in the returned map
     * @return a view of the portion of this map whose keys range from {@code
     * fromKey}, inclusive, to {@code toKey}, exclusive
     * @throws ClassCastException       if {@code fromKey} and {@code toKey}
     *                                  cannot be compared to one another using
     *                                  this map's comparator (or, if the map
     *                                  has no comparator, using natural
     *                                  ordering). Implementations may, but are
     *                                  not required to, throw this exception if
     *                                  {@code fromKey} or {@code toKey} cannot
     *                                  be compared to keys currently in the
     *                                  map.
     * @throws NullPointerException     if {@code fromKey} or {@code toKey} is
     *                                  null and this map does not permit null
     *                                  keys
     * @throws IllegalArgumentException if {@code fromKey} is greater than
     *                                  {@code toKey}; or if this map itself has
     *                                  a restricted range, and {@code fromKey}
     *                                  or {@code toKey} lies outside the bounds
     *                                  of the range
     */
    @Override
    public SortedMap<String, Type> subMap(final String fromKey,
                                          final String toKey) {
        return internalSchema.subMap(fromKey, toKey);
    }

    /**
     * Returns a view of the portion of this map whose keys are strictly less
     * than {@code toKey}.  The returned map is backed by this map, so changes
     * in the returned map are reflected in this map, and vice-versa.  The
     * returned map supports all optional map operations that this map
     * supports.
     *
     * <p>The returned map will throw an {@code IllegalArgumentException} on an
     * attempt to insert a key outside its range.
     *
     * @param toKey high endpoint (exclusive) of the keys in the returned map
     * @return a view of the portion of this map whose keys are strictly less
     * than {@code toKey}
     * @throws ClassCastException       if {@code toKey} is not compatible with
     *                                  this map's comparator (or, if the map
     *                                  has no comparator, if {@code toKey} does
     *                                  not implement {@link Comparable}).
     *                                  Implementations may, but are not
     *                                  required to, throw this exception if
     *                                  {@code toKey} cannot be compared to keys
     *                                  currently in the map.
     * @throws NullPointerException     if {@code toKey} is null and this map
     *                                  does not permit null keys
     * @throws IllegalArgumentException if this map itself has a restricted
     *                                  range, and {@code toKey} lies outside
     *                                  the bounds of the range
     */
    @Override
    public SortedMap<String, Type> headMap(final String toKey) {
        return internalSchema.headMap(toKey);
    }

    /**
     * Returns a view of the portion of this map whose keys are greater than or
     * equal to {@code fromKey}.  The returned map is backed by this map, so
     * changes in the returned map are reflected in this map, and vice-versa.
     * The returned map supports all optional map operations that this map
     * supports.
     *
     * <p>The returned map will throw an {@code IllegalArgumentException} on an
     * attempt to insert a key outside its range.
     *
     * @param fromKey low endpoint (inclusive) of the keys in the returned map
     * @return a view of the portion of this map whose keys are greater than or
     * equal to {@code fromKey}
     * @throws ClassCastException       if {@code fromKey} is not compatible
     *                                  with this map's comparator (or, if the
     *                                  map has no comparator, if {@code
     *                                  fromKey} does not implement {@link
     *                                  Comparable}). Implementations may, but
     *                                  are not required to, throw this
     *                                  exception if {@code fromKey} cannot be
     *                                  compared to keys currently in the map.
     * @throws NullPointerException     if {@code fromKey} is null and this map
     *                                  does not permit null keys
     * @throws IllegalArgumentException if this map itself has a restricted
     *                                  range, and {@code fromKey} lies outside
     *                                  the bounds of the range
     */
    @Override
    public SortedMap<String, Type> tailMap(final String fromKey) {
        return internalSchema.tailMap(fromKey);
    }

    /**
     * Returns the first (lowest) key currently in this map.
     *
     * @return the first (lowest) key currently in this map
     * @throws NoSuchElementException if this map is empty
     */
    @Override
    public String firstKey() throws NoSuchElementException {
        return internalSchema.firstKey();
    }

    /**
     * Returns the last (highest) key currently in this map.
     *
     * @return the last (highest) key currently in this map
     * @throws NoSuchElementException if this map is empty
     */
    @Override
    public String lastKey() {
        return internalSchema.lastKey();
    }

    @Override
    public Set<String> keySet() {
        return internalSchema.keySet();
    }

    @Override
    public Collection<Type> values() {
        return internalSchema.values();
    }

    @Override
    public Set<Entry<String, Type>> entrySet() {
        return internalSchema.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Schema schema = (Schema) o;

        return internalSchema.equals(schema.internalSchema);
    }

    /**
     * Hashcode, translated from the internal schema.
     */
    @Override
    public int hashCode() {
        return internalSchema.hashCode();
    }

    /**
     * Schema builder.
     */
    public static final class Builder {

        /**
         * The schema under construction.
         */
        private final Map<String, Type> constructionSchema;

        /**
         * Create a new instance of the builder class.
         */
        public Builder() {
            this.constructionSchema = new TreeMap<>();
        }

        /**
         * Add a column to this schema.
         *
         * @param name The name of the column.
         * @param type The data type of the column.
         * @return This builder.
         */
        public Builder add(final String name, final Type type) {
            constructionSchema.put(name, type);
            return this;
        }

        /**
         * Add all elements of one schema to those produced by the builder.
         *
         * @param schema The schema to add.
         * @return This builder.
         */
        public Builder add(final Schema schema) {
            this.add(schema.internalSchema);
            return this;
        }

        /**
         * Add all column/type mappings to the builder.
         *
         * @param schema The schema map to add.
         * @return This builder.
         */
        public Builder add(final Map<String, Type> schema) {
            for (String key : schema.keySet()) {
                constructionSchema.put(key, schema.get(key));
            }
            return this;
        }

        /**
         * Create an instance of this schema.
         *
         * @return The constructed schema.
         */
        public Schema build() {
            return new Schema(MapUtils.copy(constructionSchema));
        }
    }
}
