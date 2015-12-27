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

import net.krotscheck.stk.cobertura.IgnoreCoverage;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Additional, custom JUnit assertions for storm toolkit components.
 *
 * @author Michael Krotscheck
 */
public final class STKAssert {

    /**
     * Private constructor.
     */
    @IgnoreCoverage
    private STKAssert() {
    }

    /**
     * Assert that the worker can be serialized and deserialized without
     * problems. This is required for proper transfer to supervisors. Note that
     * this assertion requires proper implementation of the hashCode() and
     * equals() method.
     *
     * @param component A storm bolt or spout to test.
     * @throws Exception Testing Exceptions.
     */
    public static void assertReserializable(final Serializable component)
            throws Exception {

        // Serialize the object
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(component);
        byte[] result = baos.toByteArray();
        oos.close();

        // Deserialize the object
        ByteArrayInputStream bais = new ByteArrayInputStream(result);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object newComponent = ois.readObject();

        Assert.assertTrue(component.equals(newComponent));
    }
}
