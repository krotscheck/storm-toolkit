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

/**
 * A list of schema types used for input and output into our workers.
 *
 * @author Michael Krotscheck
 */
public enum Type {

    /**
     * Boolean type.
     */
    BOOLEAN("boolean"),

    /**
     * Single Char type.
     */
    CHAR("char"),

    /**
     * Byte type, either 1 or 0.
     */
    BYTE("byte"),

    /**
     * Short integer type.
     */
    SHORT("short"),

    /**
     * Integer type.
     */
    INT("int"),

    /**
     * Long integer type.
     */
    LONG("long"),

    /**
     * Float type.
     */
    FLOAT("float"),

    /**
     * Double float type.
     */
    DOUBLE("double"),

    /**
     * Null type.
     */
    NULL("null"),

    /**
     * String type.
     */
    STRING("string");

    /**
     * The name.
     */
    private final String name;

    /**
     * Constructor. Private
     *
     * @param typeName Name of the type.
     */
    Type(final String typeName) {
        this.name = typeName;
    }

    /**
     * Determine the schema type from a provided value.
     *
     * @param value The value to test.
     * @return The enum for the type detected.
     */
    public static Type getTypeForObject(final Object value) {
        if (value == null) {
            return Type.NULL;
        }
        if (value instanceof Boolean) {
            return Type.BOOLEAN;
        }
        if (value instanceof Character) {
            return Type.CHAR;
        }
        if (value instanceof Byte) {
            return Type.BYTE;
        }
        if (value instanceof Short) {
            return Type.SHORT;
        }
        if (value instanceof Integer) {
            return Type.INT;
        }
        if (value instanceof Long) {
            return Type.LONG;
        }
        if (value instanceof Float) {
            return Type.FLOAT;
        }
        if (value instanceof Double) {
            return Type.DOUBLE;
        }
        if (value instanceof String) {
            return Type.STRING;
        }

        // Default dropout
        return Type.STRING;
    }

    /**
     * Convert this enum to a string.
     *
     * @return The string representation of this data type.
     */
    @Override
    public String toString() {
        return name;
    }
}
