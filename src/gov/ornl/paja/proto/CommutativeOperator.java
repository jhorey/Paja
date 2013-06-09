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
 * Java libs.
 **/
import java.util.Collection;
import java.util.List;

/**
 * A communative operator defines a set of actions that can be applied in 
 * any order. For example if the actions consist of [A, B, C], then
 * a fast slot with [A, B, C] has no conflicts. 
 */ 
public interface CommutativeOperator {

    /**
     * Indicate whether a value is compatible with the commutative set. 
     * 
     * @param set The commutative set
     * @param value The new value to be added to the set
     */
    public boolean compatible(CommutativeSet set, 
			      PaxosValue value);

}