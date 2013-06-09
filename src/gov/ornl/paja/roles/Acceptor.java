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
import gov.ornl.paja.storage.PaxosStorageIterator;
import gov.ornl.paja.storage.PaxosStorageException;
import gov.ornl.paja.proto.Ballot;
import gov.ornl.paja.proto.Proposal;
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PrimitivePaxosValue;
import gov.ornl.paja.proto.PaxosState;

/**
 * Java libs.
 **/
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * The acceptor role. The primary responsibility of the acceptor is to
 * "vote" for a proposal. Typically, if enough acceptors have voted for
 * a proposal then the proposal is accepted. The acceptor also has the
 * responsibility of detecting potential conflicts (via the use of "ballots"). 
 * Basically, if it has already voted for a value with a particular ballot,
 * and receives a conflicting proposal it can send a "preemption" message. 
 *
 * @author James Horey
 */
public class Acceptor extends PaxosRole {
    /**
     * Highest ballot so far. 
     */
    private Ballot ballotNum;

    /**
     * Accepted votes.
     */
    private ConcurrentSkipListMap<Integer, Proposal> accepted;

    /**
     * Log all the errors, warnings, and messages.
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosRole.Acceptor"); 

    /**
     * @param id Unique ID of this role 
     * @param logDir Where the logs are contained
     * @param recover Indicate whether to recover from logs
     */
    public Acceptor(String id, String logDir, boolean recover) {
	super(id, logDir);
	super.setType(Role.ACCEPTOR);

	// Set the recovery mechanism.
	setRecovery(new AcceptorRecovery(this));

	if(logDir != null) {
	    // Initialize datastructures. 		    
	    init();

	    if(!recover) {
		startNewLog();
	    }
	}
    }

    /**
     * Initialize the Acceptor state.
    **/
    protected void init() {
	setType(PaxosRole.Role.ACCEPTOR);
	accepted = new ConcurrentSkipListMap<Integer, Proposal>();

	ballotNum = new Ballot(); // Create a new ballot. 
	setDiagnostic(false); // Turn off diagnostic mode. 
    }

    /**
     * Get/set the current ballot for this acceptor.
     *
     * @param ballot Ballot to set
     */
    public void setBallot(Ballot ballot) {
	ballotNum = ballot;
    }
    public Ballot getBallot() {
	return ballotNum;
    }

    /**
     * Get/set current list of accepted values. 
     *
     * @param slot Slot that has been accepted
     * @param proposal The proposal that has been accepted
     */
    protected void addAccepted(int slot, Proposal proposal) {
	accepted.put(slot, proposal);
    }
    protected Map<Integer, Proposal> getAccepted() {
	return accepted;
    }

    /**
     * Received Phase 1A message from a Proposer. Send a 
     * response back to the Proposers with the greatest
     * ballot seen so far. This will help the Proposers 
     * know if it has been preempted. 
     *
     * @param proposer The proposer that initiated this request
     * @param ballot The latest ballot known by the proposer
     * @param completeSlot The latest complete slot known by the proposer
     */
    protected void receiveP1A(Proposer proposer, Ballot ballot, int completeSlot) {
	logger.log(Level.INFO, String.format("%s: received P1A from %s (%s %d)", 
					     getID(), 
					     proposer.getID(), 
					     ballot.toString(),
					     completeSlot));

	// The acceptor always promises to use the highest ballot.
	synchronized(this) {
	    if(ballot.compare(ballotNum) == Ballot.GREATER) {
		ballotNum = ballot; // Update the ballot. 
		recovery.log(ballotNum); // Log the ballot.
	    }
	}

	// Send the accepted list to the proposer with
	// the largest ballot seen so far. 
	Collection<Proposal> proposals = accepted.values();
	Iterator<Proposal> proposalIter = proposals.iterator();

	// Calculate how many messages we must send. 
	int num = (int)Math.ceil( (double)proposals.size() / 
				  (double)PaxosMessage.MAX_PROPOSALS );

	if(num == 0) {
	    // There is no payload, but we should still send
	    // a Phase 1B reply anyway.
	    num = 1;
	}
	// else {
	//     System.out.printf("\n\nacceptor sending %d P1B replies\n\n", num);
	// }

	// Now construct & send all the messages. 
	for(int i = 0; i < num; ++i) {
	    PaxosMessage msg = new PaxosMessage();
	    msg.setSender(this);
	    msg.setState(PaxosState.Phase.PB);
	    msg.addPayload(new PrimitivePaxosValue(num));

	    synchronized(this) {
		msg.addPayload(ballotNum);	
	    }

	    // Add the remaining proposals. 
	    for(int j = 0; j < PaxosMessage.MAX_PROPOSALS && proposalIter.hasNext(); ++j) {
		msg.addPayload(proposalIter.next());
	    }
	    send(proposer, msg);
	}
    }

    /**
     * Received Phase 2A message from a Proposer. Vote for the
     * proposal if it has a sufficiently high ballot 
     *
     * @param proposer The proposer that initiated this request
     * @param proposal The proposal being voted on
     */
    protected void receiveP2AClassic(Proposer proposer, Proposal proposal) {
	// logger.log(Level.INFO, String.format("%s: received P2A (%s) (%s)", 
	// 				     getID(), 
	// 				     proposal.toString(),
	// 				     Arrays.toString(proposal.getValue())));
	// Check the proposal ballot.
	Proposal highest = null;

	int c = proposal.getBallot().compare(ballotNum);
	if(c == Ballot.GREATER || c == Ballot.SAME) {
	    if(c == Ballot.GREATER) {
		// Update the ballot.
		synchronized(this) {
		    ballotNum = proposal.getBallot();
		    recovery.log(ballotNum); // Log the change.
		}
	    }

	    // Add to the accepted if it is higher
	    // than the current highest ballot for the slot. 
	    highest = accepted.get(proposal.getSlot());
	    if(highest == null || 
	       proposal.getBallot().compare(highest.getBallot()) == Ballot.GREATER) {
		accepted.put(proposal.getSlot(), proposal);

		recovery.log(proposal); // Log the proposal.
	    }
	}

	// Do not send a confirmation back if this is a 
	// fast ANY proposal. 
	if(proposal.getEndSlot() == Integer.MIN_VALUE) {
	    // Create reply message.
	    PaxosMessage msg = new PaxosMessage();
	    msg.setSender(this);
	    msg.setState(PaxosState.Phase.CB);
	    synchronized(this) {
		msg.addPayload(ballotNum);
	    }
	    msg.addPayload(proposal);
	    // Send back a P2B message to the proposer. 
	    // We don't log this information since the proposer
	    // will just timeout and ask for it again later. 
	    send(proposer, msg);
	}
    }

    /**
     * Return an "ANY" proposal for this slot. Indicated by
     * an accepted slot number less than the proposal's slot. 
     *
     * @param slot The starting slot for the ANY proposal 
     */
    private Proposal getAnyProposal(int slot) {
	SortedMap<Integer, Proposal> lt = accepted.headMap(slot, true);

	for(Proposal any : lt.values()) { // Choose the "ANY" proposal. 
	    if(any.getEndSlot() != Integer.MIN_VALUE) {
		return any;
	    }
	}

	return null;
    }

    /**
     * We received a value from a client in fast mode. This is analoguous
     * to the P2A classic message. 
     *
     * @param client The client that initiated the proposal
     * @param proposal The proposal being voted on
     */
    protected void receiveP2AFast(PaxosRole client, Proposal proposal) {
	// We only accept fast values, if the Acceptor has already
	// accepted an "ANY" proposal in the correct range. The "ANY"
	// proposal is initiated by the Proposers. 
	Proposal p = getAnyProposal(proposal.getSlot());
	if(p != null) {
	    // Check if this is for a fast slot.
	    if(p.getEndSlot() != Integer.MIN_VALUE) {
		int c = proposal.getBallot().compare(ballotNum);
		if(c == Ballot.SAME) { // Set the value from the client. 
		    p.setValue(proposal.getValue());
		}

		// Create reply message. Since this contains
		// the current ballot, this may result in a preemption. 
		PaxosMessage msg = new PaxosMessage();
		msg.setState(PaxosState.Phase.CB);
		msg.setSender(this);
		synchronized(this) {
		    msg.addPayload(ballotNum);	
		}
		msg.addPayload(proposal);

		// Log the proposal for recovery.
		accepted.put(proposal.getSlot(), proposal);
		recovery.log(proposal); 

		// We need to send a confirmation back to the canonical Proposer. 
		send(comm.getDesirable(PaxosRole.Role.PROPOSER), msg);
		// logger.log(Level.INFO, String.format("%s: received fast value from %s (%d) (%s)", 
		// 				     getID(), 
		// 				     client.getID(), 
		// 				     proposal.getSlot(),
		// 				     Arrays.toString(proposal.getValue())));
	    }
	    else {
		logger.log(Level.INFO, String.format("%s: rejected fast value from %s (%d)", 
						     getID(), 
						     client.getID(), 
						     proposal.getSlot()));
		// We need to contact the Client so that it knows
		// to update its fast window, and to re-submit the value.
		PaxosMessage msg = new PaxosMessage();
		msg.setState(PaxosState.Phase.FASTVALUE);
		msg.setSender(this);
		msg.addPayload(proposal);
		send(client, msg);
	    }
	}
	else {
	    logger.log(Level.INFO, String.format("%s: could not find fast slot %d", 
						 getID(), proposal.getSlot()));
	}
    }

    /**
     * Send the message over the network to specific node. 
     */
    @Override public void send(PaxosRole node,
			       PaxosMessage msg) {
	super.send(node, msg);
    }

    /**
     * Handle recovery messages.
     */
    private boolean handleRecoveryMessage(PaxosRole node,
					  PaxosMessage msg) {
	// Check if this is a diagnosis message.
	if(msg.getState() == PaxosState.Phase.RECOVERA) { 
	    // Recover this acceptor from storage. 
	    recovery.recover();
	    return true;
	}

	return false;
    }

    /**
     * Receive a message from the network. 
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

	if(msg.getState() == PaxosState.Phase.PA) { 
	    // Received a Phase 1A message from a Proposer. That means
	    // the Proposer is ready to initiate the consensus protocol. 
	    List<PaxosValue> values = msg.getPayload();
	    Ballot ballot = PrimitivePaxosValue.deSerializeBallot(values.get(0));
	    PrimitivePaxosValue slot = PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(1));

	    receiveP1A((Proposer)node, ballot, slot.intValue());
	}
	else if(msg.getState() == PaxosState.Phase.CA) { 
	    // Received a Phase 2A message from a Proposer. That means
	    // the Proposer is ready to finish the consensus protocol. 
	    List<PaxosValue> values = msg.getPayload();
	    Proposal proposal = 
		PrimitivePaxosValue.deSerializeProposal(values.get(0));
	    receiveP2AClassic((Proposer)node, proposal);
	}
	else if(msg.getState() == PaxosState.Phase.FASTVALUE) { 
	    // Received a fast mode value from a Client. This is similar
	    // to receiving a Phase 2A message from a Proposer. 
	    List<PaxosValue> values = msg.getPayload();
	    Proposal proposal = 
		PrimitivePaxosValue.deSerializeProposal(values.get(0));
	    receiveP2AFast(node, proposal);	    
	}
    }

    /**
     * Handle acceptor logging & recovery. 
     **/
    class AcceptorRecovery extends PaxosRecovery {
	private Acceptor acceptor;

	/**
	 * @param acceptor Parent acceptor to perform recovery on. 
	 */
	public AcceptorRecovery(Acceptor acceptor) {
	    this.acceptor = acceptor;
	}

	/**
	 * Recover this acceptor from a crash. This means replaying
	 * the log from durable storage. 
	 **/
	private String recoverFromLog() {
	    int latestLogID = 0;
	    int highBallotLog = Integer.MIN_VALUE;
	    byte[] highBallot = null;
	    LogMessage log = null;
	    PaxosStorageIterator iter;

	    // Now open up an iterator.
	    for(iter = storage.playback(); iter.hasNext(); ) {
		try {
		    log = iter.next();
		} catch(PaxosStorageException e) {
		    // The log message was corrupted. Stop the recovery and throw
		    // an exception. 
		    e.printStackTrace();
		}

		if(Arrays.equals(log.getID(), "ballot".getBytes())) {
		    // We need the latest ballot number. 
		    if(log.getNum() > highBallotLog) {
			highBallotLog = log.getNum();
			highBallot = log.getMsg();
		    }
		}
		else if(Arrays.equals(log.getID(), "proposal".getBytes())) {
		    // Unserialize the proposal. 
		    Proposal p = new Proposal(acceptor.getID());
		    p.unSerialize(log.getMsg());

		    // Place into the accepted list.
		    acceptor.addAccepted(p.getSlot(), p);
		}
	    }

	    // Unserialize the highest ballot.
	    Ballot ballot = new Ballot();
	    ballot.unSerialize(highBallot);
	    acceptor.setBallot(ballot);

	    // Now update the storage with the latest log ID. 
	    storage.setLogID(latestLogID);

	    // Tell the storage how much data we've already
	    // read. This will help us during append operations. 
	    storage.setAppendStart(iter.getReadBytes());

	    // Return the name of the log file used during recovery. 
	    return storage.getLogFile();
	}

	/**
	 * Recover this acceptor from a crash.
	 */
	public synchronized void recover() {
	    logger.log(Level.INFO, String.format("%s: recovering state", 
						 acceptor.getID()));
	    // Set the diagnostic bit. 
	    acceptor.setDiagnostic(true);

	    // Recover from the logs.
	    String logFile = recoverFromLog();

	    // Tell the acceptor to start a new log. 
	    acceptor.reuseLog(logFile);

	    // This ends the recovery process.
	    acceptor.setDiagnostic(false);
	}
    }
}