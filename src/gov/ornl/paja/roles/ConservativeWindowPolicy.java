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

package gov.ornl.paja.roles;

/**
 * Paxos libs.
 **/
import gov.ornl.paja.proto.*;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A conservative window growing policy that grows the fast window slowly
 * depending on the number of conflicts seen. 
 */
public class ConservativeWindowPolicy extends FastWindowPolicy {
    private static final int WINDOW_SIZE = 3;
    private volatile int fastSlotStart; // Start of the fast slot window.
    private volatile int fastSlotEnd; // End of the fast slot window.
    private volatile int fastSlotUpdate;
    private volatile int numConflicts; // Number of conflicts in window. 

    public ConservativeWindowPolicy(Proposer p) {
	super(p);

	fastSlotStart = 0;
	fastSlotEnd = 0;
	fastSlotUpdate = 0;
	numConflicts = 0;
    }

    /**
     * Get the number of fast slots to skip in the next round. 
     **/
    private int getNumSkip() {
	return numConflicts - 1;
    }

    /**
     * Inform the replicas of new fast window information. 
     */
    private void sendNewFastWindow() {
	// Get the next window. 
	fastSlotStart = fastSlotEnd + getNumSkip();
	fastSlotEnd = fastSlotStart + WINDOW_SIZE;
	fastSlotUpdate = fastSlotEnd - 1;
	numConflicts = 0; // Reset the conflict count. 

	// Inform the Replicas. 
	PaxosMessage payload = new PaxosMessage();
	payload.setSender(proposer);
	payload.setState(PaxosState.Phase.FASTSLOTS);
	payload.addPayload(new PrimitivePaxosValue( fastSlotStart ));
	payload.addPayload(new PrimitivePaxosValue( fastSlotEnd ));

	proposer.multicast(PaxosRole.Role.REPLICA, payload, 
			   PaxosCommunicator.Quorum.ALL);
    }

    /**
     * Main function to update the window information. 
     *
     * @param slot The slot being updated
     * @param numConflicts Number of conflicts for the slot
     */
    public void updateWindow(int slot, int numConflicts) {
	if(slot <= fastSlotEnd &&
	   slot >= fastSlotStart) {
	    this.numConflicts = numConflicts;

	    if(slot == fastSlotUpdate) {
		sendNewFastWindow();
	    }
	}
    }

    /**
     * Inform the window policy of any changes in the available slots. 
     *
     * @param start Start of the new window
     * @param end End of the new window
     */
    public void setFastStart(int start, int end) {
	fastSlotStart = start;
	fastSlotEnd = end;
	fastSlotUpdate = end - 1;
    }

    /**
     * Get the current fast window information.
     *
     * @return Two element array. First value is the
     * start and the second value is the end of the window. 
     */
    public int[] getFastWindow() {
	int[] w = {fastSlotStart, fastSlotEnd};
	return w;
    }
}