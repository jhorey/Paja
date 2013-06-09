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
import java.util.Random;

/**
 * Paxos libs.
 **/
import gov.ornl.paja.proto.*;
import gov.ornl.paja.roles.*;

/**
 * This preemption policy assumes there should only be a single leader.
 * Hence, after learning the Proposer has been preempted, it will 
 * ask the Proposer to finish off the remaining proposals, and then de-elect iself. 
 * 
 * @author James Horey
 */ 
public class QuitPreemptionPolicy extends PreemptionPolicy {
    private static final int WAIT_WINDOW = 256;
    private static final int JITTER = 256;

    private static final Random rand = 
	new Random(System.currentTimeMillis());

    /**
     * @param proposer Parent proposer
     */
    public QuitPreemptionPolicy(Proposer proposer) {
	super(proposer);
    }

    /**
     * Indicate to the policy that a preemption occurred. 
     *
     * @param ballot The new ballot 
     */
    public void preempted(Ballot ballot) {
	// Check if the Proposer has any proposals left. 
	if(proposer.getProposals().size() > 0) {
	    // Wait a random amount of time. 
	    int w = WAIT_WINDOW + rand.nextInt(JITTER);

	    try { Thread.sleep(w); }
	    catch(InterruptedException e) { e.printStackTrace(); }

	    // Send out a new scout. 
	    proposer.startScout(ballot);
	}

	// Tell the Proposer to no longer be the leader. 
	proposer.setLeader(false);
    }

    /**
     * Indicate to the policy that everything went well. 
     */
    public void update() {
	// No update function for this policy. 
    }
}