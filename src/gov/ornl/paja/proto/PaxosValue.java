/**
 * Copyright 2013 Oak Ridge National Laboratory
 * Author: James Horey <horeyjl@ornl.gov>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
**/

package gov.ornl.paja.proto;

import java.util.Collection;

/**
 * A generic value container that encapsulates a value for state machines.
 */
public interface PaxosValue {

    /**
     * Get the value.
     *
     * @return Actual byte-array value
     */
    public byte[] getValue();

    /**
     * Get the list of values.
     *
     * @return List of all the values
     */
    public Collection<byte[]> getValues();

    /**
     * Get all the issuers associated with this value. 
     *
     * @return List of all the "issuers". 
     */
    public Collection<String> getIssuers();

    /**
     * Serialize the value.
     *
     * @return Serialized value
     */
    public byte[] serialize();

    /**
     * Instantiate from a byte array.
     *
     * @param Serialized value
     */
    public void unSerialize(byte[] data);

    /**
     * Make a copy of the data (useful for debugging).
     *
     * @return Copy of the value
     */
    public PaxosValue copy();

    /**
     * Get/set the slot. 
     **/
    public void setSlot(int s);
    public int getSlot();
}