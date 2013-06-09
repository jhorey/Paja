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
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PaxosState;
import gov.ornl.paja.proto.Proposal;

/**
 * Java libs.
 **/
import java.util.List;
import java.util.ArrayList;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Help diagnose a Paxos node. For example, to print out the 
 * state, start/stop the node, etc. 
 *
 * @author James Horey
 */
public class DiagnosticRole {
    /**
     * Parent that's using this diagnosis node. 
     */
    private PaxosRole parent;

    /**
     * Log all the errors, warnings, and messages.
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosRole.Diagnosis"); 

    /**
     * @param parent The parent node 
     */
    public DiagnosticRole(PaxosRole parent) {
	this.parent = parent;
    }

    /**
     * Perform some diagnostic action on a replica.
     *
     * @param state The command to execute
     */
    private void diagnoseReplica(PaxosState.Phase state) {
	if(state == PaxosState.Phase.CRASH) {
	    logger.log(Level.INFO, String.format("%s: crashing", parent.getID()));

	    // Stop responding to new messages.
	    parent.setDiagnostic(true);
	}
	else if(state == PaxosState.Phase.REBOOT) {
	    logger.log(Level.INFO, String.format("%s: rebooting", parent.getID()));

	    parent.init(); // Reinitialize everything.
	    parent.setDiagnostic(false); // Make it active again.
	}
	else if(state == PaxosState.Phase.PRINT) {
	    // Need to invoke Replica specific methods.
	    Replica replica = (Replica)parent;

	    // Print out the current status.
	    replica.printStateMachine();
	}
    }

    /**
     * Perform some diagnostic action on a proposer.
     *
     * @param state The command to execute
     */
    private void diagnoseProposer(PaxosState.Phase state) {
	if(state == PaxosState.Phase.CRASH) {
	    logger.log(Level.INFO, String.format("%s: crashing", parent.getID()));
	    // Stop responding to new messages.
	    parent.setDiagnostic(true);

	    // Need to invoke Proposer specific methods. 
	    Proposer proposer = (Proposer)parent;

	    // Stop all scouts and commanders.
	    proposer.stopScout();
	    proposer.stopCommanders();
	}
	else if(state == PaxosState.Phase.REBOOT) {
	    logger.log(Level.INFO, String.format("%s: rebooting", parent.getID()));

	    // Make it active again.
	    parent.init();
	}
	else if(state == PaxosState.Phase.PRINT) {
	    // Need to invoke proposer specific methods.
	    Proposer proposer = (Proposer)parent;

	    // Print out the current status.
	    System.out.printf("%s: print\n", parent.getID());

	    System.out.printf(":\n");
	    System.out.printf("===========================\n");
	    System.out.printf("%s\n", proposer.getBallot().toString());
	    System.out.printf("===========================\n");

	    System.out.printf("proposals:\n");
	    System.out.printf("===========================\n");
	    for(PaxosValue p : proposer.getProposals().values()) {
		System.out.printf("%s\n", p.toString());
	    }
	    System.out.printf("===========================\n");
	}
    }

    /**
     * Perform some diagnostic action on a acceptor.
     *
     * @param state The command to execute
     **/
    private void diagnoseAcceptor(PaxosState.Phase state) {
	if(state == PaxosState.Phase.CRASH) {
	    logger.log(Level.INFO, String.format("%s: crashing", parent.getID()));

	    // Stop responding to new messages.
	    parent.setDiagnostic(true);
	}
	else if(state == PaxosState.Phase.REBOOT) {
	    logger.log(Level.INFO, String.format("%s: rebooting", parent.getID()));

	    // Make it active again.
	    parent.init();
	}
	else if(state == PaxosState.Phase.PRINT) {
	    // Need to invoke acceptor specific methods. 
	    Acceptor acceptor = (Acceptor)parent;

	    // Print out the current status.
	    System.out.printf("%s: print\n", parent.getID());

	    System.out.printf("\nballot:\n");
	    System.out.printf("===========================\n");
	    System.out.printf("%s\n", acceptor.getBallot().toString());
	    System.out.printf("===========================\n");

	    System.out.printf("accepted:\n");
	    System.out.printf("===========================\n");
	    for(Proposal p : acceptor.getAccepted().values()) {
		System.out.printf("%s\n", p.toString());
	    }
	    System.out.printf("===========================\n\n");
	}
    }

    /**
     * Receive a message from the network. 
     *
     * @param msg Message received
     */
    public void receiveMessage(PaxosMessage msg) {
	// We always respond to diagnostic queries. 
	if(msg.getState() == PaxosState.Phase.CRASH  ||
	   msg.getState() == PaxosState.Phase.REBOOT ||
	   msg.getState() == PaxosState.Phase.PRINT) { 

	    if(parent.getType() == PaxosRole.Role.ACCEPTOR) {
		diagnoseAcceptor(msg.getState());
	    }
	    else if(parent.getType() == PaxosRole.Role.PROPOSER) {
		diagnoseProposer(msg.getState());
	    }
	    else if(parent.getType() == PaxosRole.Role.REPLICA) {
		diagnoseReplica(msg.getState());
	    }
	}
    }
}