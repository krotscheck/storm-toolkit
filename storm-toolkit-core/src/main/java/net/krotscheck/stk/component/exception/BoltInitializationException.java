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

package net.krotscheck.stk.component.exception;

/**
 * This exception is thrown when an error occurs during bolt initialization.
 */
public final class BoltInitializationException extends RuntimeException {

    /**
     * Create a new instance.
     *
     * @param message The error message.
     */
    public BoltInitializationException(final String message) {
        super(message);
    }
}
