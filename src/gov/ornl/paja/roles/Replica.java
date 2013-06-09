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
import gov.ornl.paja.storage.LogMessage;
import gov.ornl.paja.storage.PaxosStorageException;
import gov.ornl.paja.storage.PaxosStorageIterator;
import gov.ornl.paja.proto.Ballot;
import gov.ornl.paja.proto.Pair;
import gov.ornl.paja.proto.Proposal;
import gov.ornl.paja.proto.PaxosState;
import gov.ornl.paja.proto.PaxosStateMachine;
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PrimitivePaxosValue;
import gov.ornl.paja.proto.CommutativeSet;

/**
 * Java libs.
 **/
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * The Replica role. The Replica has many responsibilities. 
 * (1) Keep track of the active proposals that are being considered.
 * (2) Keep track of the fast mode window
 * (3) Apply decided proposals to the state machine
 * While it is tempting to think that Clients should interact with Proposers,
 * this is not the case. Clients primarily interact with a Replica to send
 * proposals and learn about decisions. 
 *
 * @author James Horey
 */
public class Replica extends PaxosRole {
    /**
     * Maximum number of oustanding slots that can be opened. 
     */ 
    private static int MAX_OUTSTANDING = 2048;

    private volatile int pendingSlot; // Slot of the most recently proposed. 
    private volatile int fastSlotStart; // Start of the fast slot window.
    private volatile int fastSlotEnd; // End of the fast slot window.
    private volatile int completeSlot; // Slot of the most recently completed.
    private Ballot fastBallot; // Ballot of current fast window. 
    private Map<Integer, Proposal> proposals; // Pending proposals. 
    private Map<Integer, PaxosValue> decisions; // The decisions already made. 
    private Map<Proposal, PaxosRole> clients; // List of clients waiting for proposal.    

    private volatile int truncateSlots; // To truncate old logs. 
    private PaxosStateMachine state; // Apply to the state machine. 
    private ReplicaRecovery reprec;

    /**
     * Log all the errors, warnings, and messages.
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosRole.Replica"); 

    /**
     * @param id Unique ID of this role 
     * @param logDir Where the logs are contained
     * @param recover Indicate whether the Replica should recover from logs
     */
    public Replica(String id, 
		   String logDir, 
		   boolean recover,
		   PaxosStateMachine stateMachine) {
	super(id, logDir);
	super.setType(Role.REPLICA);

	// Set the state machine. 
	state = stateMachine;

	// Sometimes the user can instantiate a Replica role
	// that is used as a "shell" to contain the ID. It
	// doesn't actually play any functional role. 
	if(logDir != null) {
	    // Set up the recovery mechanism.
	    reprec = new ReplicaRecovery(this);
	    setRecovery(reprec);

	    // Initialize datastructures.
	    init();

	    // Start a new log. 
	    if(!recover) {
		startNewLog();
	    }
	}
    }

    /**
     * Initialize datastructures.
     */
    protected void init() {
	setType(PaxosRole.Role.REPLICA);

	pendingSlot = 0;
	completeSlot = 1;
	fastSlotStart = 0;
	fastSlotEnd = 0;
	fastBallot = null;
	decisions = new ConcurrentHashMap<Integer, PaxosValue>();
	proposals = new ConcurrentHashMap<Integer, Proposal>();
	clients = new ConcurrentHashMap<Proposal, PaxosRole>();

	truncateSlots = 0; // Used for truncating slots. 
	setDiagnostic(false); // Turn off diagnostic mode. 
    }

    /**
     * Get/set the state machine. 
     *
     * @param stateMachine The state machine to use
     */
    public void setStateMachine(PaxosStateMachine stateMachine) {
	state = stateMachine;
    }
    public PaxosStateMachine getStateMachine() {
	return state;
    }

    /**
     * Ask the state machine to print out its state. Used 
     * primarily for debugging.
     */
    public void printStateMachine() {
	state.print();
    }

    /**
     * Update the fast window. If the fast window
     * is completely expired, then we can set the window
     * to anything after the last committed slot. Otherwise,
     * check if the new window grows our current window (it has
     * the same starting point). 
     *
     * @param start Proposed start of the window
     * @param end Proposed end of the window
     */
    private synchronized Pair<Integer,Integer> updateFastWindow(int size) {
	// Record the window update so that we can reconstruct the
	// pending slot upon recovery. 
	if(!diagnosticMode) {
	    recovery.log("window", new PrimitivePaxosValue(size));
	}

	// Calculate the new fast window. The start of the fast
	// window should be current pending.
	fastSlotStart = pendingSlot + 1;
	fastSlotEnd = fastSlotStart + size;

	// Now update the pending slot. 
	pendingSlot += size;

	logger.log(Level.INFO, String.format("%s: update fast mode (%d %d)", 
					     getID(), fastSlotStart, fastSlotEnd));
	return new Pair<Integer,Integer>(fastSlotStart, fastSlotEnd);
    }

    /**
     * Recover the fast window. 
     */
    private synchronized void recoverSlots(int fastSlotStart,
					   int fastSlotEnd,
					   int pending,
					   int complete) {
	this.fastSlotStart = fastSlotStart;
	this.fastSlotEnd = fastSlotEnd;
	pendingSlot = pending;
	completeSlot = complete;

	// Inform the Proposer to start fast mode
	// with the given slots. 
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.FASTSLOTS);
	msg.setSender(this);
	msg.addPayload(new PrimitivePaxosValue(fastSlotStart));
	msg.addPayload(new PrimitivePaxosValue(fastSlotEnd));
	send(PaxosRole.Role.PROPOSER, msg);
    }

    /**
     * Set the fast window size. This does not guarantee that
     * the window will be exactly the same size as requested, since
     * the window must be contiguous. 
     *
     * @param start Proposed start of the window
     * @param end Proposed end of the window
     */
    protected void setFastWindow(int size) {
	// Update the window. 
	Pair<Integer, Integer> window = updateFastWindow(size);

	// Construct a message for the Proposers.
	if(window.b > 0) {
	    PaxosMessage msg = new PaxosMessage();
	    msg.setState(PaxosState.Phase.FASTSLOTS);
	    msg.setSender(this);
	    msg.addPayload(new PrimitivePaxosValue(window.a));
	    msg.addPayload(new PrimitivePaxosValue(window.b));

	    // Inform the Proposer to start fast mode
	    // with the given slots. 
	    send(PaxosRole.Role.PROPOSER, msg);
	}
    }

    /**
     * Get the slot value for the start of the fast window. 
     * 
     * @return Start of the fast window
     */
    public int getFastWindowStart() {
	return fastSlotStart;
    }

    /**
     * Get the slot value for the end of the fast window. 
     * 
     * @return End of the fast window
     */
    public int getFastWindowEnd() {
	return fastSlotEnd;
    }

    /**
     * Remove old decisions from the list of decisions. This is used
     * to prune the Replica recovery log. This assumes that the state machine has
     * some "snapshot" to recover from. 
     *
     * @param slots The latest slots to truncate
     */
    public void truncate(int slots) {
	truncateSlots = slots;
    }
    public int getTruncateSlots() {
	return truncateSlots;
    }

    /**
     * Get/set the pending slot.
     */
    public void setPendingSlot(int slot) {
	pendingSlot = slot;
    }
    public int getPendingSlot() {
	return pendingSlot;
    }

    /**
     * Get/set the completed slot values. 
     */
    public int getCompleteSlot() {
	return completeSlot;
    }

    /**
     * Return current set of pending proposals. 
     *
     * @return Map of the pending proposals. The key
     * of the map is the slot number, and the value is
     * the actual proposal. 
     */
    public Map<Integer, Proposal> getProposals() {
	return proposals;
    }

    /**
     * Add a proposal to the list of decided slots. This means
     * that the proposal has been accepted by the Acceptors. 
     *
     * @param slot The slot that has been decided
     * @param proposal The original proposal applied to the slot
     */
    protected void addDecision(int slot, PaxosValue proposal) {
	decisions.put(slot, proposal);
    }

    /**
     * Get the list of decided proposals. 
     *
     * @return Collection of decided proposals organized by slot. 
     */
    protected Map<Integer, PaxosValue> getDecisions() {
	return decisions;
    }

    /**
     * Helper method to propose a value to the replica group.
     *
     * @param client The original client that sent the request
     * @param value The value being proposed
     */
    private void propose(PaxosRole client, byte[] value) {
	int slot = 0;
	synchronized(this) {
	    // Check to see if the number of outstanding slots is too high.
	    if( pendingSlot + 1 - completeSlot > MAX_OUTSTANDING ) {
		// Create an error message and send back to the client. 
		PaxosMessage error = PaxosMessage.ERROR_MSG(String.format("slots %d", 
									  pendingSlot + 1 - completeSlot));
		error.setSender(this);
		send(client, error);
		return;
	    }

	    // Select the lowest, unused slot number.
	    if(pendingSlot + 1 < completeSlot) {
		pendingSlot = completeSlot - 1;
	    }
	    slot = ++pendingSlot;
	}

	logger.log(Level.INFO, String.format("%s: creating new classic proposal %d",
					     getID(), slot));

	// Create a new proposal. This proposal does not have a ballot yet 
	// (the Proposer supplies the ballot). 
	Proposal prop = new Proposal(this.getID(), slot, null, value);

	// Log the proposal for recovery. That way if the Replica 
	// crashes & recovers, it can re-construct the active proposals.
	// After reconstructing the proposals, it will send them to the Proposers again. 
	recovery.log("proposal", prop);

	// Add to our list of active proposals. 
	synchronized(this) {
	    proposals.put(slot, prop);
	    clients.put(prop, client);
	}

	// Construct a propose message.
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.PROPOSE);
	msg.setSender(this);
	msg.addPayload(prop);

	// Contact a canonical Proposer.
	send(PaxosRole.Role.PROPOSER, msg);
    }

    /**
     * Transmit back to the client that we are operating under fast mode.
     * The response includes the slots to use for fast mode. 
     *
     * @param client The original client that sent the request
     * @param value The value being proposed
     */
    private void fastMode(PaxosRole client, byte[] value) {
	// Determine if we are already past the fast window start. 
	int actualStart = completeSlot > fastSlotStart? 
	    completeSlot : fastSlotStart;

	logger.log(Level.INFO, String.format("%s: send client fast window (%d %d)", 
					     getID(), 
					     actualStart, 
					     fastSlotEnd));

	// Construct a message to send back to the client. The
	// message needs to contain the pertinent fast window information. 
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.FASTSLOTS);
	msg.setSender(this);
	msg.addPayload(new PrimitivePaxosValue(actualStart));
	msg.addPayload(new PrimitivePaxosValue(fastSlotEnd));
	msg.addPayload(new PrimitivePaxosValue(value));

	// We need to externally synchronize the fast ballot. 
	synchronized(this) {
	    msg.addPayload(fastBallot);
	}

	// Contact the client.
	send(client, msg);
    }

    /**
     * Helper method to apply the value to the state machine. 
     *
     * @param slot The slot for this decision
     * @param values The values to apply to the state machine
     */
    private synchronized void perform(int slot, Collection<byte[]> values) {
	if(values.size() > 1) {
	    logger.log(Level.INFO, String.format("%s: perform slot %d with %d values", 
						 getID(), slot, values.size()));	    
	}

	// First make sure we have not already applied 
	// the command to the state machine.
	if(values != null && 
	   state != null && 
	   decisions.get(slot) != null) {
	    for(byte[] v : values) {
		state.apply(v);
	    }
	}

	// Update the complete slot count.
	++completeSlot;
    }

    /**
     * The proposer is trying to recover the latest completed slot. 
     */
    private void receiveReqDecisions(Proposer proposer) {
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.RECOVERB);
	msg.setSender(this);

	// Complete slot is the slot we are must currently apply.
	// So the slot beforehand is the one that is actually complete.
	msg.addPayload(new PrimitivePaxosValue(completeSlot - 1)); 
	msg.addPayload(new PrimitivePaxosValue(fastSlotStart)); 
	msg.addPayload(new PrimitivePaxosValue(fastSlotEnd)); 

	// Contact each proposer. 
	send(proposer, msg);	
    }

    /**
     * Determine if the window in the fast window. 
     */
    private boolean inFastWindow(int start, int end) {
	if(start >= fastSlotStart &&
	   end < fastSlotEnd ) {
	    return true;
	}

	return false;
    }

    /**
     * Received a proposal message from a client. 
     * Send a message to the proposers. 
     *
     * @param client Client making the request
     * @param value Value being proposed
     * @param slot The slot for the value
     */
    public void receiveRequest(PaxosRole client,
			       byte[] value,
			       int slot) {
	// We need to figure out if the client needs to
	// learn and operate in fast mode or not. 
	if(slot < fastSlotEnd) {
	    fastMode(client, value); // Send fast slot window information. 
	}
	else {
	    // Initiate the propose subroutine.
	    propose(client, value);
	}
    }

    /**
     * Received a "help" message from another Replica that is
     * actively recovering its state. We need to hand over any
     * decisions made while this replica was recovering and also
     * send over some fast window information. 
     *
     * @param replica The replica making the recovery request
     * @param pending The latest known pending slot by the requesting replica
     * @param complete The latest known complete slot by the requesting replica
     */
    public void receiveRecover(Replica replica,
			       int pending,
			       int complete) {
	logger.log(Level.INFO, String.format("%s: received recover request (%s)", 
					     getID(), 
					     replica.getID()));

	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.RECOVERC);
	msg.setSender(this);

	// First add the fast window info. 
	msg.addPayload(new PrimitivePaxosValue(fastSlotStart));
	msg.addPayload(new PrimitivePaxosValue(fastSlotEnd));

	// Add the pending and complete slots. 
	msg.addPayload(new PrimitivePaxosValue(pending));
	msg.addPayload(new PrimitivePaxosValue(complete));

	// Now check if we have any active proposals that the
	// other Replica does not. 
	List<PaxosValue> values = new ArrayList<>(proposals.size());
	for(Integer slot : proposals.keySet()) {
	    if(slot.intValue() > pending) {
		values.add(new PrimitivePaxosValue(proposals.get(slot).serialize()));
	    }
	}
	msg.addPayload(new PrimitivePaxosValue(values.size()));
	for(PaxosValue v : values) {
	    msg.addPayload(v);
	}
	
	// Now add the decisions that we have but the other Replica
	// does not. Just use the slot value for confirmation. 
	values.clear();
	for(PaxosValue p : decisions.values()) {
	    if(p.getSlot() > complete) {
		values.add(p);
	    }
	}
	msg.addPayload(new PrimitivePaxosValue(values.size()));
	for(PaxosValue v : values) {
	    msg.addPayload(v);
	}

	// Send back to the requesting replica. 
	send(replica, msg);
    }

    /**
     * Helper method to apply completed decisions to the state machine. Not all decided
     * proposals can be immediately applied to the state machine. Only decisions
     * that are contiguous can be applied. 
     *
     * @param proposal The decided proposal. 
     */
    protected void applyDecision(PaxosValue proposal) {
	// We need to figure out which decisions we
	// can apply to the state machine. We can only
	// apply the decisions up to the "completeSlot" slot.
	PaxosValue p = null;
	boolean didApply = false;
	for(;;) {
	    // logger.log(Level.INFO, String.format("%s: apply %s %d", 
	    // 					 getID(), proposal.toString(), completeSlot));
	    p = decisions.get(completeSlot);
	    if(p != null) {
		didApply = true;
		PaxosValue c = null;

		// Is there another proposal under consideration with the 
		// same slot number? This could happen if there are multiple 
		// replicas and a replica disagreed on the slot number for a 
		// particular proposal. If so, we need to re-propose this command.
		if( proposal != null &&
		    (c = proposals.get(proposal.getSlot())) != null &&
		    (!c.equals(proposal)) ) {
		    logger.log(Level.INFO, String.format("%s: must re-assign slot (%s) (%s)", getID(), 
							 c.toString(), proposal.toString()));
		    propose(clients.get(c), c.getValue());
		    break;
		}

		// Act upon the proposal.
		perform(p.getSlot(), p.getValues());

		// Remove from the list of active proposals. 
		proposals.remove(p.getSlot());

		// Inform the client that this proposal is now CLOSED. 
		PaxosRole client = clients.get(p);
		if(client != null) {
		    //
		    // DISABLED FOR NOW UNTIL WE FIGURE OUT A BETTER WAY
		    // OF SENDING CLOSED MESSAGES FOR FAST PROPOSALS!!
		    // 
		    // PaxosMessage msg = new PaxosMessage();
		    // msg.setState(PaxosState.Phase.CLOSED);
		    // msg.setSender(this);
		    // msg.addPayload(p);
		    // send(client, msg);

		    // Remove the client from the active list. 
		    clients.remove(p);
		}
	    }
	    else {
		break;
	    }
	}

	// if(!didApply) {
	//     logger.log(Level.INFO, String.format("%s: can't apply slot %d yet (complete:%d pending:%d)", 
	// 					 getID(), proposal.getSlot(), completeSlot, pendingSlot));
	// }
    }

    /**
     * Received a message from a Proposer telling us that one of the
     * active proposals has been decided. This means that the proposal
     * has been accepted by a quorum of Acceptors, and that the value
     * should be applied to the state machine.
     *
     * @param proposer The Proposer that has decided the proposal
     * @param proposal The proposal that is decided
     */
    public void receiveDecision(Proposer proposer,
				PaxosValue proposal) {
	// It's possible to receive multiple decision messages for the 
	// same proposal (for example if the proposer has crashed and 
	// recovered, it may re-send the decision message). Make sure we
	// only apply the decision once. 
	if(decisions.get(proposal.getSlot()) == null) {
	    // Add to our list of decisions and store the information in
	    // the recovery log. That way if the Replica crashes & restarts,
	    // it will know which decisions have already been applied (and so
	    // won't re-apply the same decisions). 
	    addDecision(proposal.getSlot(), proposal);

	    // Differentitate classic and fast recoveries. 
	    if(proposal instanceof Proposal) {
		recovery.log("decision", proposal);
	    }
	    else {
		recovery.log("fdecision", proposal);
	    }

	    // It's possible that we are receiving a decision initiated by
	    // another replica. In that case our pending slot value may
	    // be smaller than these proposal slots. Increase as necessary. 
	    pendingSlot = proposal.getSlot() > pendingSlot? proposal.getSlot() : pendingSlot;
	
	    // Apply the decision. 
	    applyDecision(proposal);
	}
	// else {
	//     logger.log(Level.INFO, String.format("%s: already decided (%s) (%s)", 
	//     					 getID(), 
	//     					 proposal.toString(),
	//     					 decisions.get(proposal.getSlot()).toString()));
	// }
    }

    /**
     * Handle recovery messages. This method just dispatches 
     * to more specific handlers. 
     * 
     * @param node The Paxos node that original sent the message
     * @param msg The message received
     */
    private boolean handleRecoveryMessage(PaxosRole node,
					  PaxosMessage msg) {
	// Check if this is a diagnosis message.
	if(msg.getState() == PaxosState.Phase.RECOVERP) {
	    // Received a "help" message from a Proposer asking
	    // for the latest completed slot. 
	    receiveReqDecisions((Proposer)node);
	    return true;
	}
	else if(msg.getState() == PaxosState.Phase.RECOVERA) {
	    // Received a message from the network to recover this
	    // Replica from the recovery log.
	    List<PaxosValue> values = msg.getPayload();

	    // See if the network has given us any hints regarding
	    // truncated slots. 
	    if(values.size() > 0) {
		PrimitivePaxosValue v = 
		    PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));
		truncate(v.intValue());
	    }

	    // Recover the replica from the log. 
	    reprec.recover();
	    return true;
	}
	else if(msg.getState() == PaxosState.Phase.RECOVERB) {
	    // Received request from another Replica to help in recovery.
	    List<PaxosValue> values = msg.getPayload();

	    // De-serialize the primitive values.
	    PrimitivePaxosValue v0 = 
		    PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));
	    PrimitivePaxosValue v1 = 
		    PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(1));

	    // Perform recovery.
	    receiveRecover((Replica)node, v0.intValue(), v1.intValue());
	    return true;
	}
	else if(msg.getState() == PaxosState.Phase.RECOVERC) {
	    // Received a reply from the other Replicas to help
	    // this Replica recover. 
	    reprec.receiveFromNeighbors((Replica)node, msg.getPayload());
	    return true;
	}

	return false;
    }

    /**
     * Receive a message from the network. This method just dispatches
     * to more specific handlers. 
     *
     * @param node The Paxos node that original sent the message
     * @param msg The message received
     */
    @Override public void receiveMessage(PaxosRole node,
					 PaxosMessage msg) {
	// Handle recovery messages.
	if(handleRecoveryMessage(node, msg)) {
	    return;
	}

	// The node is in diagnostic mode, so we can't
	// operate over new data. 
	if(inDiagnostic()) {
	    return;
	}

	if(msg.getState() == PaxosState.Phase.DECISION) {
	    // A classic decision message is sent by Proposers 
	    // to tell us that a value/slot has been accepted.
	    // The message contains the original proposal.
	    PaxosValue v = msg.getPayload().get(0);
	    Proposal proposal = PrimitivePaxosValue.deSerializeProposal(v);

	    // logger.log(Level.INFO, String.format("%s: receive classic decision for slot %d",
	    // 					 getID(), proposal.getSlot()));
	    receiveDecision((Proposer)node, proposal);
	}
	else if(msg.getState() == PaxosState.Phase.FASTDECISION) {
	    // A fast decision message is sent by Proposers
	    // to tell us that a value/slot has been accepted.
	    PaxosValue v = msg.getPayload().get(0);
	    CommutativeSet set = PrimitivePaxosValue.deSerializeSet(v);

	    // logger.log(Level.INFO, String.format("%s: receive fast decision for slot %d",
	    // 					 getID(), set.getSlot()));
	    receiveDecision((Proposer)node, set);
	}
	else if(msg.getState() == PaxosState.Phase.REQUEST) {
	    // A request message is sent by a Client to try to propose a new value.
	    // The Replica can either decide to send a proposal to the Acceptors,
	    // or to send a message back to the Client to operate in fast mode.  
	    List<PaxosValue> values = msg.getPayload();
	    PrimitivePaxosValue v1 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));
	    PrimitivePaxosValue v2 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(1));

	    receiveRequest(node, v1.arrayValue(), v2.intValue());
	}
	else if(msg.getState() == PaxosState.Phase.FASTSLOTS) {
	    // A request sent by a window policy to set a new fast window.
	    // The Replica uses this request to figure out how large of
	    // a window to create, and to inform the Proposers. 
	    List<PaxosValue> values = msg.getPayload();

	    // De-serialize the primitive values. 
	    PrimitivePaxosValue v1 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));

	    // Initiate fast mode. 
	    setFastWindow(v1.intValue());
	}
	else if(msg.getState() == PaxosState.Phase.FASTVALUE) {
	    // Received confirmation from the Proposer
	    // regarding the proposed fast window.
	    synchronized(this) {
		fastBallot = PrimitivePaxosValue.deSerializeBallot(msg.getPayload().get(0));
		recovery.log(fastBallot);
	    }
	}
    }

    /**
     * Handle Replica logging & recovery. 
     */
    class ReplicaRecovery extends PaxosRecovery {
	private Replica replica;

	public ReplicaRecovery(PaxosRole r) {
	    replica = (Replica)r;
	}

	/**
	 * Use the log to catch up during recovery. 
	 */
	private String recoverFromLog() {
	    int latestLogID = 0;
	    LogMessage log = null;
	    List<PaxosValue> decided = new ArrayList<>();
	    PaxosStorageIterator iter;

	    for(iter = storage.playback(); iter.hasNext(); ) {
		try {
		    log = iter.next();
		} catch(PaxosStorageException e) {
		    // The log message was corrupted. Stop the recovery and throw
		    // an exception. 
		    e.printStackTrace();
		}

		if(log.getNum() > latestLogID) {
		    latestLogID = log.getNum();
		}

		if(Arrays.equals(log.getID(), "proposal".getBytes())) {
		    // We need to add this proposal to our list of
		    // active proposals. This will also update the 
		    // "pendingSlot" value appropriately
		    Proposal p = new Proposal(replica.getID());
		    p.unSerialize(log.getMsg());
		    recoverProposalFromLog(p);
		}
		else if(Arrays.equals(log.getID(), "decision".getBytes())) {
		    // We need to add this proposal to our list of 
		    // decided proposals. This will also update the
		    // "completeSlot" value appropriately. 
		    Proposal p = new Proposal(replica.getID());
		    p.unSerialize(log.getMsg());

		    decided.add(p);
		}
		else if(Arrays.equals(log.getID(), "fdecision".getBytes())) {
		    // We need to recover a commutative set. Otherwise, this is
		    // very similar to a classic recovery. 
		    CommutativeSet set = new CommutativeSet();
		    set.unSerialize(log.getMsg());

		    decided.add(set);
		}
		else if(Arrays.equals(log.getID(), "ballot".getBytes())) {
		    // This is the fast ballot recovery.
		    synchronized(this) {
			fastBallot = new Ballot();
			fastBallot.unSerialize(log.getMsg());
		    }
		}
		else if(Arrays.equals(log.getID(), "window".getBytes())) {
		    // Recover the fast window update. 
		    ByteBuffer buffer = ByteBuffer.wrap(log.getMsg());
		    int type = buffer.getInt();
		    int vv = buffer.getInt();
		    PrimitivePaxosValue v = 
		    	 new PrimitivePaxosValue(vv);

		    updateFastWindow(v.intValue());
		}
	    }
	    
	    // We need to recover all the decided *after* the active
	    // proposals have been recorded, otherwise things might 
	    // get very confusing. 
	    for(PaxosValue p : decided) {
		recoverDecidedFromLog(p);
	    }

	    // Now that we reconstructed the list of active &
	    // decided proposals, apply the decided ones to update
	    // "completedSlot" values. 
	    decideRecoveredProposals();

	    // Now update the storage with the latest log ID. 
	    storage.setLogID(latestLogID);

	    // Tell the storage how much data we've already
	    // read. This will help us during append operations. 
	    storage.setAppendStart(iter.getReadBytes());

	    // Return the name of the log file used during recovery. 
	    return storage.getLogFile();
	}

	/**
	 * Add a proposal to our list of decided. 
	 */
	private void recoverDecidedFromLog(PaxosValue proposal) {
	    addDecision(proposal.getSlot(), proposal);
	}

	/**
	 * Apply the recovered decisions to the state machine. 
	 */
	private void decideRecoveredProposals() {
	    for(PaxosValue p : decisions.values()) {
		applyDecision(p);
	    }
	}

	/**
	 * Add a proposal to our list of active.
	 */
	private void recoverProposalFromLog(Proposal proposal) {
	    // Update the pending slot. This value should always
	    // increase, but may not start from zero (since the log
	    // may have been flushed, etc.). 
	    if(proposal.getSlot() > pendingSlot) {
		pendingSlot = proposal.getSlot();
	    }
		
	    // Add to our list of active proposals. 
	    proposals.put(proposal.getSlot(), proposal);
	}

	/**
	 * Re-transmit any active proposals that have not been
	 * decided. We must do this just in case the Replica crashed
	 * before being able to send anything to a Proposer. 
	 */
	private void transmitRecoveredProposals() {
	    for(Proposal p : proposals.values()) {
		PaxosMessage msg = new PaxosMessage();
		msg.setState(PaxosState.Phase.PROPOSE);
		msg.setSender(Replica.this);
		msg.addPayload(p);

		// Contact a canonical Proposer.
		send(PaxosRole.Role.PROPOSER, msg);
	    }
	}

	/**
	 * Send a message to other Replicas to ask for any
	 * any decisions made while this Replica was recovering. 
	 *
	 * @param slot The slot that we have already recovered from the log. 
	 */
	private void recoverFromNeighbors(int pending,
					  int complete) {
	    PaxosMessage msg = new PaxosMessage();
	    msg.setState(PaxosState.Phase.RECOVERB);
	    msg.setSender(replica);
	    msg.addPayload(new PrimitivePaxosValue(pending));
	    msg.addPayload(new PrimitivePaxosValue(complete));

	    // Send a message to another replica. 
	    replica.send(PaxosRole.Role.REPLICA, msg);
	}

	/**
	 * Received a copy reply from another replica.
	 * Finish the recovery phase. 
	 *
	 * @param replica The replica sending the reply
	 * @param values The actual responses. 
	 */
	public void receiveFromNeighbors(Replica replica,
					 List<PaxosValue> values) {
	    // First extract the fast window information.
	    int i = 0;
	    PrimitivePaxosValue v0 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(i++));
	    PrimitivePaxosValue v1 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(i++));

	    // Now get the pending and complete slots. 
	    PrimitivePaxosValue v2 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(i++));
	    PrimitivePaxosValue v3 = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(i++));

	    // Now set the fast window. This will also have the
	    // effect of contacting the Proposer to update its fast window. 
	    replica.recoverSlots(v0.intValue(), 
				 v1.intValue(),
				 v2.intValue(),
				 v3.intValue());

	    // Now see if there are any active proposals that our
	    // neighbors know about but we don't. 
	    v0 = PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(i++));
	    for(int j = 0; j < v0.intValue(); ++j) {
		Proposal proposal = PrimitivePaxosValue.deSerializeProposal(values.get(i++));

		// Add to our list of active proposals. 
		proposals.put(proposal.getSlot(), proposal);
	    }

	    // Now see if there are any "decided" proposals that our
	    // neighbors know about but we don't. 
	    v0 = PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(i++));
	    for(int j = 0; j < v0.intValue(); ++j) {
		Proposal proposal = PrimitivePaxosValue.deSerializeProposal(values.get(i++));
		recoverDecidedFromLog(proposal);
	    }

	    // Now re-apply all the actions. 
	    decideRecoveredProposals();
	}

	/**
	 * Recover this replica from the recovery log and other
	 * active replicas. This process will recover the pending
	 * slots, decisions, and fast window information. 
	 */
	public synchronized void recover() {
	    // Clear any intermediate datastructures. 
	    replica.init();

	    // Set the diagnostic bit. 
	    replica.setDiagnostic(true);

	    // First catch up to where we were before we crashed.
	    String logFile = recoverFromLog();
	    logger.log(Level.INFO, String.format("%s: done recover from logs", replica.getID()));

	    // There may have been decisions that were made while this Replica was 
	    // down. Also, try to recovery fast slots.
	    recoverFromNeighbors(pendingSlot, completeSlot);

	    // Now there may be some active proposals that have not
	    // been decided yet. Since we don't know if the other Replicas
	    // have proposed those values, we should re-transmit just in case. 
	    transmitRecoveredProposals();

	    // Tell the replica to start a new log. 
	    replica.reuseLog(logFile);

	    // Finally, allow this replica to be available again. 
	    replica.setDiagnostic(false);
	    logger.log(Level.INFO, String.format("%s: done recovering state", replica.getID()));
	}
    }    
}