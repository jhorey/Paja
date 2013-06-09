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
 * Java libs.
 **/
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * The Client role. Used by clients to initiate proposals to replicas. 
 *
 * @author James Horey
 */
public class Client extends PaxosRole {
    /**
     * Keep track of the current fast proposal window. 
     */
    private int fastSlotStart;
    private int fastSlotEnd;
    private Ballot fastBallot;

    /**
     * Keep track of the pending fast proposals the client
     * has already submitted. 
     */
    private volatile int pendingSlot;
    private Map<Integer, Proposal> proposals;

    /**
     * Keep track of every single proposal sent. 
     */
    private volatile int allSlots;

    /**
     * General debug logger. 
     */
    private static Logger logger = 
	Logger.getLogger("PaxosRole.Client"); 

    /**
     * @param id Unique identifier for this client
     * @param logDir Directory to store log information
     **/
    public Client(String id, String logDir) {
	super(id, logDir);
	super.setType(PaxosRole.Role.CLIENT);

	init(); // Initialize datastructures.
    }

    /**
     * Initialize datastructures.
     */
    protected void init() {
	pendingSlot = 0;
	allSlots = 0;
	proposals = new ConcurrentHashMap<Integer, Proposal>();
	resetFastWindow();
    }

    /**
     * Reset the fast window. 
     */
    private void resetFastWindow() {
	synchronized(this) {
	    fastSlotStart = 0;
	    fastSlotEnd = 0;
	    fastBallot = null;
	}
    }

    /**
     * Helper method to propose a value in fast mode to the replica group.
     *
     * @param value The value being proposed
     * @param ballot The ballot associated with this proposal
     */
    private synchronized void fastPropose(byte[] value, Ballot ballot) {
	// Select the lowest, unused slot number. 
	int slot = ++pendingSlot;

	// Create a new proposal.
	Proposal prop = new Proposal(this.getID(), slot, ballot, value);
	prop.setEndSlot(fastSlotEnd); // To indicate a fast slot. 

	// Add to our list of proposals. 
	proposals.put(slot, prop);

	// Construct a propose message.
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.FASTVALUE);
	msg.setSender(this);
	msg.addPayload(prop);

	// Log the information. 
	logger.log(Level.INFO, String.format("%s: submitting fast value %s (%d)", 
					     getID(), 
					     Arrays.toString(prop.getValue()), 
					     prop.getSlot()));

	// Contact a fast quorum of acceptors. 
	multicast(PaxosRole.Role.ACCEPTOR, msg, PaxosCommunicator.Quorum.FAST);
    }

    /**
     * Perform a classic proposal.
     */
    private void classicPropose(byte[] value) {
	System.out.printf("classic propose\n");
    }

    /**
     * Send the message over the network to specific node. The client must override
     * the default implementation since it must decide whether to send in fast mode
     * (to a quorum of Acceptors) or classic mode (to a Replica). 
     * 
     * @param node The Paxos node to send to
     * @param msg Message to send to the node
     */
    @Override public void send(PaxosRole node,
			       PaxosMessage msg) {
	if(comm != null) {
	    msg.setSender(this);

	    // First we need to know if we are operating in fast mode.
	    allSlots++;
	    int slot = pendingSlot + 1;
	    if(slot < fastSlotEnd &&
	       slot >= fastSlotStart &&
	       fastSlotEnd > fastSlotStart) {
		PrimitivePaxosValue v = (PrimitivePaxosValue)msg.getPayload().get(0);
		fastPropose(v.arrayValue(), fastBallot);
	    }
	    else {
		logger.log(Level.INFO, String.format("%s: send in classic mode (%d %d) (%s)", 
						     getID(), fastSlotStart, fastSlotEnd, msg.getState()));
		// We are operating in classic mode, so just send the message. 
		msg.addPayload(new PrimitivePaxosValue(allSlots));
		comm.send(node, msg);
	    }
	}
    }
    /**
     * Receive a message from the network. 
     *
     * @param node The Paxos node that original sent the message
     * @param msg The message received
     */
    @Override public void receiveMessage(PaxosRole node,
					 PaxosMessage msg) {
	if(msg.getState() == PaxosState.Phase.CLOSED) {
	    // A closed message is sent by the Replica to tell us that
	    // the proposal has been accepted and applied to the state machine. 
	    Proposal proposal = PrimitivePaxosValue.deSerializeProposal(msg.getPayload().get(0));
	    logger.log(Level.INFO, String.format("%s: received closed (%s)", getID(), proposal.toString()));
	}
	else if(msg.getState() == PaxosState.Phase.FASTSLOTS) {
	    // A fast slots message is sent by the Replica to tell the client
	    // to operate in fast mode. That means we should send the values directly 
	    // to the Acceptors.
	    List<PaxosValue> v = msg.getPayload();
	    PrimitivePaxosValue p = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)v.get(0));
	    PrimitivePaxosValue q = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)v.get(1));
	    PrimitivePaxosValue a = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)v.get(2));

	    // Retrieve the fast window. 
	    synchronized(this) {
		fastBallot = PrimitivePaxosValue.deSerializeBallot(v.get(3));
		
		if(p.intValue() > fastSlotStart) {
		    fastSlotStart = p.intValue();
		}

		if(q.intValue() > fastSlotEnd) {
		    fastSlotEnd = q.intValue();
		}

		if(fastSlotStart - 1 > pendingSlot) {
		    pendingSlot = fastSlotStart - 1;
		}
	    }

	    byte[] value = a.arrayValue();

	    // Now we need to perform a fast proposal.
	    logger.log(Level.INFO, String.format("%s: received fast slots (%d %d)", 
						 getID(), fastSlotStart, fastSlotEnd));

	    if(pendingSlot + 1 < fastSlotEnd) {
		fastPropose(value, fastBallot);
	    }
	    else {
		classicPropose(value);
	    }
	}
	else if(msg.getState() == PaxosState.Phase.FASTVALUE) {
	    // We've been contacted by an Acceptor to notify us 
	    // that our fast window information is old. Just
	    // send the proposal back to the Replica. 
	    logger.log(Level.INFO, String.format("%s: received fast window reject from %s", 
						 getID(), node.getID()));
	    resetFastWindow();
	    send(PaxosRole.Role.REPLICA, msg);
	}
	else if(msg.getState() == PaxosState.Phase.ERROR) {
	    // The client has received an error message from
	    // a Replica. This means the proposal was not submitted
	    // (probably too many outstanding proposals). Inform
	    // the Paxos group. 
	    getCommunicator().failureCallback(msg);
	}
    }
}