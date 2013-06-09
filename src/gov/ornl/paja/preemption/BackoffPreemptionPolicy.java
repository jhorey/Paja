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
 * Paxos libs.
 **/
import gov.ornl.paja.proto.*;
import gov.ornl.paja.roles.*;

/**
 * Java libs.
 **/
import java.util.Random;

/**
 * This preemption policy "backs off" after a preemption. That way the 
 * colliding proposer can finish, and this proposer can try again later. 
 * 
 * @author James Horey
 */
public class BackoffPreemptionPolicy extends PreemptionPolicy {
    private static final int MIN_WAIT = 4096;
    private static final int SMALL_WINDOW = 256;
    private static final int JITTER = 128;

    private int window; 
    private static final Random rand = 
	new Random(System.currentTimeMillis());

    /**
     * @param proposer Parent proposer
     */
    public BackoffPreemptionPolicy(Proposer proposer) {
	super(proposer);
	window = SMALL_WINDOW; // No window by default. 
    }

    /**
     * Indicate to the policy that a preemption occurred. 
     *
     * @param ballot The new ballot 
     */
    public void preempted(Ballot ballot) {
	// Wait a random amount of time. 
	int w = MIN_WAIT + rand.nextInt(JITTER) + window;

	try { Thread.sleep(w); }
	catch(InterruptedException e) { e.printStackTrace(); }

	// Send out a new scout. 
	proposer.startScout(ballot);

	// Grow the wait window aggressively. 
	window *= 4;
	System.out.printf("%s: waiting %s ms\n", proposer.getID(), window);
    }

    /**
     * Indicate to the policy that everything went well. 
     */
    public void update() {
	// Shrink the wait window conservatively. 
	if(window <= SMALL_WINDOW) {
	    window = SMALL_WINDOW; 
	}
	else {
	    window -= SMALL_WINDOW;
	}
    }
}