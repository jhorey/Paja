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

package gov.ornl.paja.preemption;

/**
 * Java libs.
 **/
import java.util.Collection;
import java.util.List;

/**
 * Paxos libs.
 **/
import gov.ornl.paja.proto.*;
import gov.ornl.paja.roles.*;

/**
 * Interface for different preemption policies. 
 * 
 * @author James Horey
 */ 
public abstract class PreemptionPolicy {
    protected Proposer proposer;

    /**
     * @param proposer Parent proposer
     */    
    public PreemptionPolicy(Proposer proposer) {
	this.proposer = proposer;
    }

    /**
     * Indicate to the policy that a preemption occurred. 
     *
     * @param ballot The new ballot 
     */
    public abstract void preempted(Ballot ballot);


    /**
     * Indicate to the policy that everything went well. 
     */
    public abstract void update();
}