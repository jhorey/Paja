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

/**
 * An abstract class representing a generic distributed state machine.
 * These state machines are the thing that eventually receives and
 * applies the value from the consensus protocol. 
 * 
 * @author James Horey
 */
public interface PaxosStateMachine {

    /**
     * Apply the supplied value to the state machine. 
     */
    public void apply(byte[] value);

    /**
     * Get the output associated with the most recently transition.
     */
    public Object getValue();

    /**
     * Print out the current state of the state machine. 
     */
    public void print();
}