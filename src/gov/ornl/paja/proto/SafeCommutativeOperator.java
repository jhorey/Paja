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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;

/**
 * The safe operator. Assume that no operations are commutative. 
 */ 
public class SafeCommutativeOperator implements CommutativeOperator {

    /**
     * Indicate whether a value is compatible with the commutative set. 
     * 
     * @param set The commutative set
     * @param value The new value to be added to the set
     */
    @Override public boolean compatible(CommutativeSet set, 
					PaxosValue value) {
	// Nothing in the set yet, so anything is compatible. 
	if(set.isEmpty()) {
	    return true;
	}

	// This implementation is pretty simple. Only the same
	// binary values are compatible with each other. 
	for(Proposal p : set.getProposals()) {
	    if(Arrays.equals(p.getValue(), 
			     value.getValue())) {
		return true;
	    }
	    else {
		return false;
	    }
	}

	return false;
    }
}