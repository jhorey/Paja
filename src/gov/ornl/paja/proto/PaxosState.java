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
 * Keep track of the current state in the protocol.
 * 
 * @author James Horey
 */
public class PaxosState {
    /**
     * Paxos protocol states. 
     **/
    public static enum Phase 
    {
	PA,           // Made by proposer
	    PB,       // Made by acceptor
	    CA,       // Made by proposer
	    CB,       // Made by acceptor
	    REQUEST,  // Made by clients
	    FASTSLOTS,// Number of fast slots 
	    FASTVALUE,// Value of a fast slot
	    PROPOSE,  // Made by replica
	    DECISION, // Made by proposers
	    FASTDECISION, // Made by proposers
	    CLOSED,   // Made by replica
	    ELECT,    // Elect a new Proposer leader
	    DEMOTE,   // Demote the Proposer
	    RECOVERP,  // Initiate recovery.
	    RECOVERA,  // Initiate recovery.
	    RECOVERB,  // Initiate recovery.
	    RECOVERC,  // Initiate recovery.
	    RECOVERD,  // Initiate recovery.
	    ERROR,     // Error message. 
	    CRASH,  // Kill the role after ACK'ing (debugging)
	    REBOOT,     // Restart the role (debugging)
	    PRINT,      // Print current state (debugging)
	    UNKNOWN 
    };
}