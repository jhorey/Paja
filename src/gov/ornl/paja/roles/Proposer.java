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
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PrimitivePaxosValue;
import gov.ornl.paja.proto.CommutativeSet;
import gov.ornl.paja.proto.CommutativeOperator;
import gov.ornl.paja.proto.SafeCommutativeOperator;
import gov.ornl.paja.proto.ContiguousTree;
import gov.ornl.paja.preemption.PreemptionPolicy;
import gov.ornl.paja.preemption.QuitPreemptionPolicy;

/**
 * Java libs.
 **/
import java.util.Timer;
import java.util.TimerTask;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.HashMap;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.nio.ByteBuffer;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * The Proposer role. The primary responsibility of the Proposer
 * is to initiate the Paxos protocol with the Acceptors. This means
 * keep track of the state of active proposals, the current ballot
 * number, and potentially dealing with conflicts (usually by informing
 * the clients or replicas). 
 *
 * @author James Horey
 */
public class Proposer extends PaxosRole {
    /**
     * Timeout to wait for the Phase 1 & 2 replies (measured in seconds). 
     */
    protected static final int TIMEOUT = 5;

    /**
     * Current fast window. The Proposer needs to know this
     * information for two reasons:
     * (1) When it receives a Phase 2 reply, it knows whether the reply
     * should be treated in fast mode or not
     * (2) When it re-proposes messages, it knows what mode to send in. 
     */
    private volatile int fastSlotStart;
    private volatile int fastSlotEnd;

    /**
     * Keep track of all the completed slots. The Proposer needs to know
     * this so that it doesn't re-propose decided slots. This particular
     * datastructure "compresses" the numerical slot values (i.e., it only
     * keeps track of the ranges). 
     */
    private ContiguousTree completedSlots;

    /**
     * Keep track of all the active (i.e., non-decided) proposals.
     */
    private NavigableMap<Integer, Proposal> proposals;

    /**
     * Keep track of proposals that have been re-proposed. That way
     * once a proposal has been re-proposed, it will not be submitted again.
     * This might happen since re-proposals are triggered by collisions. 
     */
    private List<Proposal> reproposed;

    /**
     * Handle to the scout. The scout handles Phase 1 protocol. 
     * There should only be a single scout for a proposer. 
     */
    private Scout scout;

    /**
     * Keep track of all the commanders. The commander handles the
     * Phase 2 protocol. There may be multiple commanders for a single
     * proposer since each proposed value triggers Phase 2.  
     */
    private ConcurrentSkipListMap<PaxosValue, Commander> commanders;

    /**
     * Used to store all the intermediate Phase 1B replies. We must do
     * this since a Phase 1 reply may be broken into multiple replies
     * (since a single packet may not be able to hold all the data)
     */
    private Map<Ballot, List<PaxosMessage>> pbReplies;

    /**
     * Latest ballot number known by this proposer. 
     */
    private Ballot ballotNum;

    /**
     * Indicates whether this proposer is a leader.
     */
    private volatile boolean leader;

    /**
     * Indicate whether this proposer is "active". A proposer can only
     * become active if it completes Phase 1. The only proposers that
     * starts Phase 1 are "leaders". 
     */
    private volatile boolean active;

    /**
     * Commutative operation for general paxos. This operation determines
     * if two "conflicting" values associated with a single slot can actually
     * be applied in an arbitrary order. 
     */
    private CommutativeOperator commutativeOp;

    /**
     * The preemption policy determines what the proposer does when it
     * receives a "preemption" message. A preemption message basically
     * tells the proposer that another proposer is the leader. The default
     * behavior is to quit (although one can imagine alternative strategies
     * such as reclaiming the leadership, or possibly even just increasing
     * the ballot number and trying again). 
     */
    protected PreemptionPolicy preemptPolicy;

    /**
     * Proposer logging & recovery mechanism. 
     */
    private ProposerRecovery proprec;

    /**
     * Log all the errors, warnings, and messages.
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosRole.Proposer"); 

    /**
     * @param id Unique ID of this role 
     * @param logDir Where the logs are contained
     * @param recover Indicate whether to recover from logs
     */
    public Proposer(String id, String logDir, boolean recover) {
	super(id, logDir);
	super.setType(Role.PROPOSER);

	// Set up the preemption policy. 
	// preemptPolicy = new BackoffPreemptionPolicy(this);
	preemptPolicy = new QuitPreemptionPolicy(this);

	// Set up the commutative operations for General Paxos. 
	// By default none of the operators are commutative. 
	commutativeOp = new SafeCommutativeOperator();

	// Set up the recovery mechanism.
	proprec = new ProposerRecovery(this);
	setRecovery(proprec);

	// Initialize datastructures.
	init();

	if(!recover &&
	   logDir != null) {
		startNewLog();
	}
    }

    /**
     * Initialize datastructures.
     */
    protected void init() {
	setType(PaxosRole.Role.PROPOSER);

	completedSlots = new ContiguousTree();
	fastSlotStart = -1;
	fastSlotEnd = -1;
	leader = false;
	scout = null;
	commanders = new ConcurrentSkipListMap<PaxosValue, Commander>();
	proposals = new ConcurrentSkipListMap<>(); 
	reproposed = new ArrayList<>(); 
	ballotNum = new Ballot(getID());
	pbReplies = new HashMap<>();
	active = false;

	setDiagnostic(false); // Turn off diagnostic mode. 
    }

    /**
     * Get/set the commutative operator.
     *
     * @param op The commutative operator. 
     */
    public void setCommutativeOperator(CommutativeOperator op) {
	commutativeOp = op;
    }
    public CommutativeOperator getCommutativeOperator() {
	return commutativeOp;
    }

    /**
     * Set the leader status.
     *
     * @param leader True if this node is the leader
     */
    public void setLeader(boolean leader) {
	this.leader = leader;
    }

    /**
     * Get the acceptors. Used to get correct confirmations. 
     *
     * @param List of all the currently known acceptors. 
     */
    public List<PaxosRole> getAcceptors() {
	return super.getCommunicator().getAcceptors();
    }

    /**
     * Start the proposer (i.e., initiates Phase 1 of the Paxos protocol). 
     * This usually happens after the Proposer is elected to be leader. The 
     * Proposer will not do anything else otherwise. 
     */
    public void start() {
	if(scout == null) {
	    startScout(ballotNum);
	}
    }

    /**
     * Get/set the ballot.
     */
    public void setBallot(Ballot ballot) {
	ballotNum = ballot;
    }
    public Ballot getBallot() {
	return ballotNum;
    }

    /**
     * Return current set of active proposals. 
     *
     * @return Map of active proposals. Keys are the slots
     * and values are the actual proposals. 
     */
    public Map<Integer, Proposal> getProposals() {
	return proposals;
    }

    /**
     * Return the commander responsible for this proposal. 
     *
     * @param proposal The proposal associated with the commander
     * @param range Whether the proposal is for a ranged "ANY" proposal
     * @return Commander associated with the proposal. 
     */
    private Commander getCommander(Proposal proposal, boolean range) {

	if(range) { // We want any proposal less than or equal
	    Map.Entry<PaxosValue, Commander> p = commanders.floorEntry(proposal);
	    if(p != null) {
		return p.getValue();
	    }
	    else {
		return null;
	    }
	}
	else { // We want the exact proposal
	    return commanders.get(proposal);
	}
    }

    /**
     * Start a scout for this ballot. 
     *
     * @param ballotNum The ballot to use for this scout. 
     */
    public synchronized void startScout(Ballot ballotNum) {
	scout = new Scout(this, ballotNum); 
	scout.start(); 
    }

    /**
     * Stop the scout. 
     */
    public synchronized void stopScout() {
	if(scout != null) {
	    scout.stop();
	}
    }

    /**
     * Start a commander for a proposal. This is used to initiate the
     * second phase of the Paxos protocol. 
     *
     * @param ballotNum The ballot to use with this proposal
     * @param proposal The proposal to be submitted
     * @param quorumType The quorum type (FAST or CLASSIC)
     * @param justValue Inidicate whether we need to transmit the entire
     * proposal or just the value (which can happen if the Proposer needs
     * to send a fast value). 
     */
    private void startCommander(Ballot ballotNum, 
				Proposal proposal, 
				PaxosCommunicator.Quorum quorumType, 
				boolean justValue) {
	// Get any previous commander. If we are (re)submitting a 
	// fast proposal, then we should search a ranged commander.
	Commander newCmd = new Commander(this, proposal, ballotNum);
	Commander cmd = 
	    commanders.putIfAbsent(proposal, newCmd);

	if(cmd == null) {
	    // We inserted a new commander, so we must
	    // set the proposal ballot correctly. 
	    proposal.setBallot(ballotNum);
	    cmd = newCmd;
	} 
	else { 
	    // Use a pre-existing commander. 
	    logger.log(Level.INFO, String.format("%s: using pre-existing commander for %s", 
						 getID(), proposal.toString()));
	}

	// Assign the proposal to the commander and start. 
	cmd.setProposal(proposal);
	cmd.setQuorum(quorumType);
	if(justValue) {
	    cmd.startValue();
	}
	else {
	    cmd.start(); 
	}
    }

    /**
     * Stop all the active commanders. 
     */
    public void stopCommanders() {
	for(Commander c : commanders.values()) {
	    c.stop();
	}
    }

    /**
     * Get the next available slot.
     *
     * @param slot The current slot that's already being used
     */
    public int nextAvailableSlot(int slot) {
	if(proposals.size() > 0) {
	    Integer ns = proposals.lastKey();
	    ns = ns.intValue() + 1;

	    if(slot >= fastSlotEnd ||
	       ns >= fastSlotEnd) { // In classic mode. Ask the Replica.
		return -1;
	    }
	    else { // Next fast slot. 
		if(slot >= ns) {
		    return slot + 1;
		}
		else {
		    return ns;
		}
	    }
	} else { // No proposals yet.
	    return slot + 1;
	}
    }

    /**
     * Determine the quorum policy for this proposal.
     */
    private PaxosCommunicator.Quorum getQuorumPolicy(Proposal proposal) {
	PaxosCommunicator.Quorum quorum;
	if(proposal.getSlot() >= fastSlotStart &&
	   proposal.getSlot() < fastSlotEnd) {
	    quorum = PaxosCommunicator.Quorum.FAST;
	}
	else {
	    quorum = PaxosCommunicator.Quorum.SIMPLE;
	}

	return quorum;
    }

    /**
     * Re-submit the proposal. This can be a bit tricky since
     * we need to choose a new slot value. The new slot value
     * may or may not be classic. 
     *
     * @param proposal Proposal to re-submit
     */
    private void rePropose(Proposal proposal) {
	// Check if this proposal has already been re-proposed. 
	// If so, we can safely ignore it. 
	for(Proposal r : reproposed) {
	    if(r.equals(proposal)) {
		return;
	    }
	}
	reproposed.add(proposal);

	// Get the next available slot. 
	int slot = nextAvailableSlot(proposal.getSlot());
	// This is a fast proposal. Modify the proposal to
	// make us the issuer and set the proper slot value. 
	// Then propose the value (which adds it to the active list
	// and starts the commander). 
	if(slot != -1 &&
	   isFastWindow(slot)) {
	    logger.log(Level.INFO, String.format("%s: reproposing %s to slot %s", 
						 getID(), 
						 proposal, 
						 slot));

	    Proposal np = (Proposal)proposal.copy();
	    np.setSlot(slot);
	    np.setIssuer(getID());

	    propose(np, true);
	}
	else {
	    logger.log(Level.INFO, String.format("%s: reproposing %s in classic mode", 
						 getID(), 
						 proposal)); 
	    
	    // This is a classic proposal. Pretend we are client, and
	    // send the replica a request. 
	    PaxosMessage msg = new PaxosMessage();
	    msg.setSender(this);
	    msg.setState(PaxosState.Phase.REQUEST);
	    msg.addPayload(new PrimitivePaxosValue(proposal.getValue()));
	    msg.addPayload(new PrimitivePaxosValue(fastSlotEnd + 1));
	    send(PaxosRole.Role.REPLICA, msg);
	}
    }

    /**
     * The Proposer has received a new "propose" message from the client. 
     * That means we need to add the proposal to the active list, and then
     * actually start Phase 2 of the protocol (assuming this proposer is allowed to). 
     * 
     * @param proposal Proposal to submit
     * @param justValue Inidicate whether we need to transmit the entire
     * proposal or just the value (which can happen if the Proposer needs
     * to send a fast value). 
     */
    public void propose(Proposal proposal, boolean justValue) {
	if(proposals.get(proposal.getSlot()) == null) {
	    proposals.put(proposal.getSlot(), proposal);
	}

	if(active) { 
	    // Proposer is active, so spawn a commander.
	    startCommander(ballotNum, proposal, getQuorumPolicy(proposal), justValue);
	}
    }

    /**
     * Is the slot a fast slot? 
     */
    protected boolean isFastWindow(int slot) {
	if(slot >= fastSlotStart &&
	   slot <= fastSlotEnd) {
	    return true;
	}
	else {
	    return false;
	}
    }

    /**
     * Recover the fast window information from the Phase 1B messages. 
     */
    private void recoverFastWindow(List<Proposal> props) {
	int startSlot = -1;
	int endSlot = -1;

	for(Proposal p : props) {
	    if(p.getEndSlot() != Integer.MIN_VALUE) {
		// This is a fast slot. Check the bounds. 
		if(p.getSlot() > startSlot) {
		    startSlot = p.getSlot();
		    endSlot = p.getEndSlot();
		}
	    }
	}

	// Set the fast window if it looks newer than ours. 
	if(startSlot > fastSlotStart ||
	   endSlot > fastSlotEnd) {
	    logger.log(Level.INFO, String.format("%s: recovering fast window (%d %d)", 
						 getID(), startSlot, endSlot));

	    fastSlotStart = startSlot;
	    fastSlotEnd = endSlot;
	}
    }

    /**
     * The Proposer has received a request to start fast mode. Assuming
     * that this Proposer is active (i.e., finished Phase 1), this will
     * initiate a Phase 2 message with an "ANY" value for the slots in the
     * fast window range. 
     *
     * @param replica The replica initiating the fast mode
     * @param fastSlotStart Start of the proposed fast window
     * @param fastSlotEnd End of the proposed fast window
     */
    public void fastMode(Replica replica, 
			 int fastSlotStart, 
			 int fastSlotEnd) {

	if(this.fastSlotStart == fastSlotStart &&
	   this.fastSlotEnd == fastSlotEnd) {
	    return; // Not an actual update. 
	}

	logger.log(Level.INFO, String.format("%s: accepted fast proposal (%d %d)", 
					     getID(), fastSlotStart, fastSlotEnd));
	    
	// Update the window. 
	this.fastSlotStart = fastSlotStart;
	this.fastSlotEnd = fastSlotEnd;

	// Create a new ranged proposal for the Acceptors. 
	Proposal prop = new Proposal(this.getID(), 
				     fastSlotStart, 
				     fastSlotEnd,
				     ballotNum, 
				     "ANY".getBytes());
	    
	// Add to list of proposals & start if possible. 
	propose(prop, false);

	// Send a confirmation back to the Replica. 
	PaxosMessage msg = new PaxosMessage();
	msg.setSender(this);
	msg.setState(PaxosState.Phase.FASTVALUE);
	msg.addPayload(ballotNum);
	send(replica, msg);
    }

    /**
     * Return the highest ballot proposal. 
     */
    protected static Proposal getHighest(Proposal proposal, 
					 Collection<Proposal> candidates) {
	Proposal high = proposal;

	if(candidates != null) {
	    for(Proposal c : candidates) {
		if(high == null ||
		   c.getBallot().compare(high.getBallot()) == Ballot.GREATER) {
		    high = c;
		}
	    }
	}

	return high;
    }

    /**
     * The Proposer has received an "adopted" message from the
     * the Scouts. That means we can move onto Phase 2. 
     */
    protected void executePhase2(Ballot ballot, 
				 Map<Integer, List<Proposal>> pvals,
				 Map<Integer, Boolean> freshStart,
				 Map<Integer, Boolean> didRecover) {
	// Now that we are done with Phase 1, we can enter Phase 2. 
	active = true; 
	Map<Integer, Proposal> transmit = new HashMap<>();

	// These are the user proposals. 
	for(Proposal clientProposal : proposals.values()) {
	    transmit.put(clientProposal.getSlot(), clientProposal);
	}

	if(pvals.size() > 0) {
	    for(Integer s : pvals.keySet()) {
		// Only consider fresh slots that did not undergo recovery. 
		if(freshStart.get(s) == Boolean.TRUE &&
		   didRecover.get(s) != Boolean.TRUE) {
		    // Check if there are any proposals ready for submission.
		    // If so, we will need to update its value. 
		    Proposal clientProposal = proposals.get(s);
		    if(clientProposal != null) {
			// Get a list of all proposals with this slot value. 
			List<Proposal> candidates = pvals.get(s);
			Proposal high = Proposer.getHighest(clientProposal, candidates);

			// Need to transmit this value. 
			transmit.put(s, high);
		    }
		}

		// We are finished acting on this slot. 
		freshStart.put(s, Boolean.FALSE);
	    }
	}

	// Transmit all the proposals.
	for(Proposal p : transmit.values()) {
	    startCommander(ballotNum, p, getQuorumPolicy(p), false); 
	}
    }

    /**
     * The Propose has received an "preempted" message from 
     * either a Scout or Commander. This means that another Proposer
     * with a higher ballot has initiated a proposal for the same
     * slot. This proposer should either give up or try again (depending
     * on the preemption policy). 
     */
    protected void preempted(Ballot ballot, Proposal proposal) {
	if(ballot.compare(ballotNum) == Ballot.GREATER) {
	    logger.log(Level.INFO, String.format("%s: preempted by ballot %s", getID(), ballot.toString()));

	    active = false; // Enter the passive phase. 
	    synchronized(this) {
		ballotNum.increment(ballot); // Increase our ballot num.
	    }

	    // Log the ballot for recovery. That way when the Proposer 
	    // recovers, it will have its own latest ballot number. This is
	    // necessary because the Proposer may actually have the highest
	    // ballot of all Proposers. If that is true, then some proposals
	    // may not be accepted (since everything will be preempted). 
	    recovery.log(ballotNum);

	    // Was this preemption caused during Phase 1 or 2?
	    if(proposal != null) {
		logger.log(Level.INFO, String.format("%s: preempted during Phase 2", getID()));
		// Need to remove the specific commander.
		Commander cmd = commanders.remove(proposal);
		if(cmd != null) {
		    cmd.stop();
		}

		// Check if this proposal is under consideration.
		if(proposals.get(proposal.getSlot()) == null) {
		    // This is a preemption message caused by a fast slot. 
		    // Modify the end slot so that the Acceptor recognizes it
		    // as a complete proposal and back a reply. 
		    proposal.setEndSlot(Integer.MIN_VALUE);

		    // Add back to our list of proposals,
		    // so that it will be re-transmitted. 
		    proposals.put(proposal.getSlot(), proposal);
		}
	    }

	    // Consult with the preemption policy to figure
	    // out when to try again. This is used so that we
	    // don't spawn off a bunch of preemptions. 
	    preemptPolicy.preempted(ballotNum);
	}
    }

    /**
     * Get the highest slot value from the list of proposals.
     */
    private int getHighestSlot(Collection<PaxosValue> proposals) {
    	int ns = Integer.MIN_VALUE;

    	for(PaxosValue p : proposals) {
    	    if(p.getSlot() > ns) {
    		ns = p.getSlot();
    	    }
	}

	return ns;
    }

    /**
     * The proposal has been decided. We must notify the replicas
     * so that they can commit the data. 
     *
     * @param value The paxos value that is being decided
     */
    public void decided(PaxosValue value) {
	// Send a decision message to all the replicas.
	PaxosMessage payload = new PaxosMessage();
	payload.setSender(this);
	payload.addPayload(value);

	// Determine if we are deciding a classic proposal
	// or a fast commutative set. 
	if(value instanceof Proposal) {
	    payload.setState(PaxosState.Phase.DECISION);
	}
	else {
	    payload.setState(PaxosState.Phase.FASTDECISION);
	}

	// Multicast the message to the replicas. 
	multicast(PaxosRole.Role.REPLICA, payload, PaxosCommunicator.Quorum.ALL);

	// Remove from list of pending.
	proposals.remove(value.getSlot());

	// Remove from the active commanders, but only do this
	// for actual proposals. 
	if(value instanceof Proposal) {
	    commanders.remove(value);
	}

	// Remember that we have completed this slot. This is not
	// recorded in the log, so that if we need to recover, we will
	// "re-decide" this value. 
	completedSlots.add(value.getSlot(), value.getIssuers());
    }

    /**
     * Responsible for executing the Phase 1 protocol.
     */
    class Scout {
	private final Proposer parent; // Parent proposer. 
	private final Ballot ballotNum; // Ballot num for this scout. 
	private volatile boolean reachedQuorum; // Have we reached a quorum yet? 
	private List<PaxosRole> waiting; // The acceptors we are waiting for. 
	private ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Pair<Acceptor, Proposal>>> quorum;
	private ConcurrentHashMap<Integer, Boolean> freshStart;
	private Timer timer;

	/**
	 * Initialize the scout for a single run. 
	 **/ 
	public Scout(Proposer p, Ballot b) {
	    parent = p;
	    ballotNum = b;
	    reachedQuorum = false;

	    quorum = new ConcurrentHashMap<>();
	    freshStart = new ConcurrentHashMap<>();

	    // The waiting list is externally synchronized, since
	    // we only need to support "remove" and "size" operations. 
	    waiting = new LinkedList<>(parent.getAcceptors());
	}

	/**
	 * Start the scout transitions. 
	 */
	public void start() {
	    executePhase1();
	}

	/**
	 * Stop this scout. 
	 */
	public void stop() {
	    timer.cancel();
	}

	/**
	 * Start Phase 1A. 
	 * Contact a majority of the acceptors, and wait for the replies.
	 */
	private void executePhase1() {
	    logger.log(Level.INFO, String.format("%s: start P1A (%s)", getID(), ballotNum.toString()));

	    // Create the message payload. 
	    final PaxosMessage payload = new PaxosMessage();
	    payload.setSender(parent);
	    payload.setState(PaxosState.Phase.PA);
	    payload.addPayload(ballotNum);
	    payload.addPayload(new PrimitivePaxosValue(completedSlots.getLeastContiguous()));

	    // Give the acceptors a certain amount of time to reply. 
	    synchronized(this) {
		timer = new Timer();
		timer.schedule(new TimerTask() {
			public void run() { // Timer has expired. Fail. 
			    timerFailureCallback(payload);
			}
		    }, TIMEOUT * 1000);
	    }

	    // Contact the majority of acceptors. 
	    parent.multicast(PaxosRole.Role.ACCEPTOR, payload);
	}

	/**
	 * Have not heard back from the acceptors. Retransmit the
	 * proposal to the acceptors. 
	 *
	 * @param payload The payload to retransmit. 
	 */
	protected void timerFailureCallback(final PaxosMessage payload) {
	    logger.log(Level.INFO, String.format("%s: re-start P1A (%s)", getID(), ballotNum.toString()));

	    // Restart the timer. 
	    synchronized(this) {
		timer.schedule(new TimerTask() {
			public void run() { // Timer has expired. Fail. 
			    timerFailureCallback(payload);
			}
		    }, TIMEOUT * 1000);
	    }

	    // Re-transmit the payload. 
	    parent.multicast(PaxosRole.Role.ACCEPTOR, payload); 	    
	}

	/**
	 * Organize the received proposals by slot.
	 */
	private synchronized void organizeBySlot(Acceptor acceptor,
						 List<Proposal> proposals) {
	    for(Proposal prop : proposals) {
		ConcurrentLinkedQueue<Pair<Acceptor, Proposal>> q = 
		    quorum.get(prop.getSlot());

		if(q == null) {
		    // This slot has not been used before.
		    freshStart.put(prop.getSlot(), Boolean.TRUE);
		    q = new ConcurrentLinkedQueue<Pair<Acceptor, Proposal>>();
		    quorum.put(prop.getSlot(), q);
		}

		Pair<Acceptor, Proposal> pair = new Pair<Acceptor, Proposal>(acceptor, prop);
		q.add(pair);
	    }
	}

	/**
	 * Attempt to perform any recovery. This may means either
	 * sending additional decision messages or re-initiating Phase 2. 
	 **/
	private synchronized void resendPhase2(int slot, 
					       Collection<Acceptor> acceptors,
					       Collection<Proposal> proposals,
					       Map<Integer, Boolean> didRecover) {
	    int completeSlot = completedSlots.getLeastContiguous();
	    if(slot > completeSlot) {
		logger.log(Level.INFO, String.format("%s: perform recovery (%d > %d)", 
						     getID(), slot, completeSlot));

		// Different acceptors may return different accepted
		// values for the same slot. Select the highest ballot one.
		Proposal high = Proposer.getHighest(null, proposals);

		// Check if we have majority response from acceptors.
		if(PaxosCommunicator.SUCCESS == 
		   PaxosCommunicator.isQuorum(acceptors.size(), 
					      parent.getAcceptors().size(), 
					      getQuorumPolicy(high))) {
		    logger.log(Level.INFO, String.format("%s: re-transmit decision messages", getID()));

		    // Transmit decision messages to the Replicas.
		    if(high != null) {
			decided(high);
		    }

		    // Record that we recovered this proposal.		    
		    didRecover.put(slot, Boolean.TRUE);
		}
		else {
		    // We only have a minority response. Re-initiate Phase 2 with these Proposals. 
		    if(high != null) {
			logger.log(Level.INFO, String.format("%s: re-transmit Phase 2 (%s)", getID(), high.toString()));
			startCommander(ballotNum, high, getQuorumPolicy(high), false);
		    }

		    // Record that we recovered this proposal.		    
		    didRecover.put(slot, Boolean.TRUE);
		}
	    }
	}

	/**
	 * Received Phase 1B message from an Acceptor. Once the Scout has received
	 * enough messages from the Acceptors, we can move onto Phase 2 of the
	 * protocol. Since the Acceptors may send messages associated with arbitrary
	 * slots, we must keep track of all this information on a per-slot basis. 
	 *
	 * Note that this there are many shared variables used in this method. However,
	 * this method assumes that it is synchronized externally by the message handling.
	 */
	private void receiveP1B(Acceptor acceptor, 
				Ballot ballot, 
				List<Proposal> replies) {
	    logger.log(Level.INFO, String.format("%s: received P1B from %s (%s)", 
						 getID(), 
						 acceptor.getID(), 
						 ballot.toString()));

	    // Check if this is the same ballot we had sent out. 
	    if(ballot.compare(ballotNum) == Ballot.SAME) {
		int numAcceptors;
		int numWaiting;

		// Remove from the waiting list. 
		waiting.remove(acceptor);
		numWaiting = waiting.size();
		numAcceptors = parent.getAcceptors().size();

		// Organize all the received proposals by slot. We need to
		// keep track of how many acceptors responded for a particular slot.
		organizeBySlot(acceptor, replies);

		// We need a response from the majority of acceptors.
		if(reachedQuorum ||
		   PaxosCommunicator.SUCCESS != 
		   PaxosCommunicator.isQuorum(numAcceptors - numWaiting, 
					      numAcceptors, 
					      PaxosCommunicator.Quorum.SIMPLE)) {
		    return;
		}

		// Indicate that we've reached a quorum so that we don't 
		// perform some action twice. 
		reachedQuorum = true;

		// Cancel the timer. 
		timer.cancel();

		// Recover any fast window information. 
		parent.recoverFastWindow(replies);

		// For a given slot, how many acceptors have sent us a P1B message? 
		Map<Integer, List<Proposal>> bySlot = new HashMap<Integer, List<Proposal>>();
		Map<Integer, Boolean> didRecover = new HashMap<Integer, Boolean>();

		for(Integer slot : quorum.keySet()) {
		    Set<Acceptor> acceptors = new HashSet<Acceptor>();
		    List<Proposal> proposals = new ArrayList<Proposal>();
		    
		    for(Pair<Acceptor,Proposal> pair : quorum.get(slot)) {
			acceptors.add(pair.a);
			proposals.add(pair.b);
		    }

		    // Organize proposals by slot. 
		    bySlot.put(slot, proposals);

		    // See if we need to resend either decision or Phase 2 messags.
		    if(freshStart.get(slot) == Boolean.TRUE) {
			resendPhase2(slot.intValue(), acceptors, proposals, didRecover);
		    }
		}
		
		// Initiate Phase 2 for our own proposals. 
		if(leader) {
		    parent.executePhase2(ballotNum, bySlot, freshStart, didRecover);
		}
	    }
	    else {
		// Cancel the timer. 
		timer.cancel();

		// Not the same ballot. Must be a higher ballot
		// number. Send a pre-empted message to the 
		// leader with the new ballot number.
		parent.preempted(ballot, null);
	    }
	}
    }

    /**
     * Responsible for executing the Phase 2 protocol. 
     **/
    class Commander {
	private final Proposer parent; // Parent proposer. 
	private final Ballot ballotNum; // Ballot number for this commander.
	private Proposal proposal;  // Proposal we are sending out. 
	private Map<Integer, CommutativeSet> fastValues;
	private Map<Integer, Set<PaxosRole>> classicValues;
	private Timer timer;
	private PaxosCommunicator.Quorum quorum; // Quorum method. 

	public Commander(Proposer p,
			 Proposal pr,
			 Ballot b) {
	    parent = p;
	    proposal = pr;
	    ballotNum = b;
	    fastValues = new HashMap<>();
	    classicValues = new HashMap<>();
	    quorum = PaxosCommunicator.Quorum.SIMPLE;
	    timer = null;
	}

	/**
	 * Set the proposal.
	 */
	public void setProposal(Proposal p) {
	    proposal = p;
	}

	/**
	 * Start the commander transitions. 
	 */
	public void start() {
	    stop(); // Cancel any running timers.
	    executePhase2A(false); // Start Phase 2. 
	}

	/**
	 * Start the commander, but do not expect a reply.
	 */
	public void startValue() {
	    stop(); // Cancel any running timers.
	    executePhase2A(true); // Start Phase 2. 
	}

	/**
	 * Stop this commander. 
	 */
	public void stop() {
	    synchronized(this) {
		if(timer != null) {
		    timer.cancel();
		}
	    }
	}

	/**
	 * Set the quorum type.
	 */
	public void setQuorum(PaxosCommunicator.Quorum q) {
	    quorum = q;
	}

	/**
	 * Start Phase 2A. 
	 **/
	private void executePhase2A(boolean justValue) {
	    logger.log(Level.INFO, String.format("%s: start P2A (%s)", 
						 getID(), proposal.toString()));

	    // Construct the message payload. 
	    final PaxosMessage payload = new PaxosMessage();
	    payload.setSender(parent);
	    payload.addPayload(proposal);

	    if(justValue) {
		payload.setState(PaxosState.Phase.FASTVALUE);
	    }
	    else {
		payload.setState(PaxosState.Phase.CA);
	    }

	    // Give the acceptors a certain amount of time to reply, 
	    // unless it is a Phase 2 "ANY" message. 
	    if(!justValue && proposal.getEndSlot() == Integer.MIN_VALUE) {

		synchronized(this) {
		    timer = new Timer();
		    timer.schedule(new TimerTask() {
			    public void run() { // Timer has expired. Fail. 
				timerFailureCallback(payload);
			    }
			}, TIMEOUT * 1000);
		}
	    }

	    // Multi-cast to the acceptors. We don't need to log
	    // this in recovery, since the Replica will just send us
	    // the proposal again on recovery.
	    parent.multicast(PaxosRole.Role.ACCEPTOR, payload, quorum); 
	}

	/**
	 * Have not heard back from the acceptors. We should retransmit
	 * a LEARN message so they send back more replies. 
	 **/
	protected void timerFailureCallback(final PaxosMessage payload) {
	    logger.log(Level.INFO, String.format("%s: re-start P2A (%s)", getID(), proposal.toString()));

	    // Restart the timer. 
	    synchronized(this) {
		timer.schedule(new TimerTask() {
			public void run() { // Timer has expired. Fail. 
			    timerFailureCallback(payload);
			}
		    }, TIMEOUT * 1000);
	    }

	    // Re-transmit the paylaod.
	    parent.multicast(PaxosRole.Role.ACCEPTOR, payload); 	    
	}

	/**
	 * Indicate whether this proposal has already been decided. 
	 */
	private boolean alreadyDecided(Proposal proposal) {
	    // Just check if this slot is already in the decided list. 
	    return completedSlots.contains(proposal.getSlot());
	}

	/**
	 * Indicate whether this fast proposal is conflicting or has
	 * already been re-proposed. 
	 */
	private boolean isConflicting(Ballot ballot,
				      Proposal proposal) {
	    return !completedSlots.hasAuthored(proposal.getIssuer());
	}

	/**
	 * Helper method to handle P2B messages in classic mode.
	 * Basically we need to check if there is a simple quorum. 
	 */
	private synchronized void handleP2BClassic(Acceptor acceptor, 
						   Ballot ballot, 
						   Proposal proposal) {
	    // We store the replies by the originating node. 
	    Set<PaxosRole> nodes = classicValues.get(proposal.getSlot());
	    if(nodes == null) {
		nodes = new HashSet<PaxosRole>();
		classicValues.put(proposal.getSlot(), nodes);
	    }
	    nodes.add(acceptor);

	    // Check if we have a simple quorum of acceptors. 
	    if(PaxosCommunicator.SUCCESS == 
	       PaxosCommunicator.isQuorum(nodes.size(),                
					  parent.getAcceptors().size(),
					  PaxosCommunicator.Quorum.SIMPLE)) {               
		// logger.log(Level.INFO, String.format("%s: received classic quorum (%s)", 
		// 				     getID(), 
		// 				     proposal.toString()));
		// Tell the preempt policy that we have successfully completed a 
		// round of voting. 
		parent.preemptPolicy.update();

		// Cancel the timer. 
		synchronized(this) {
		    if(timer != null) {
			timer.cancel();
		    }
		}

		// Finally inform the replicas that the proposal
		// has been decided and should be acted upon. 
		parent.decided(proposal);
	    }
	}

	/**
	 * Helper method to handle P2B messages in fast mode. 
	 * We organize the values into commutative sets and apply
	 * values from a single set. We re-propose as necessary. 
	 */
	private synchronized void handleP2BFast(Acceptor acceptor, 
						Ballot ballot, 
						Proposal proposal) {
	    // For any fast slot, there can only be a single commutative
	    // set that gets accepted. If the value can be forced into
	    // the set, then we are ok. Otherwise, we must re-propose this value. 
	    // We must take extra care to make sure when we re-submit the value
	    // that it is entered into the active proposal list so that we don't
	    // send redundant proposals. 
	    CommutativeSet set = fastValues.get(proposal.getSlot());
	    if(set == null) {
		set = new CommutativeSet();
		set.setSlot(proposal.getSlot());
		fastValues.put(proposal.getSlot(), set);
	    }

	    if(commutativeOp.compatible(set, proposal)) {
		set.add(proposal, acceptor.getID());

		// Check if all the elements in the set make a quorum. 
		boolean allQuorum = true;
		for(Proposal p : set.getProposals()) {
		    if(PaxosCommunicator.SUCCESS != 
		       PaxosCommunicator.isQuorum(set.getNumAcceptors(p), 
						  parent.getAcceptors().size(),
						  PaxosCommunicator.Quorum.FAST)) {
			allQuorum = false;
			break;
		    }
		}

		if(allQuorum) {
		    // We have an entire quorum on this commutative set.
		    // Finally inform the replicas that the proposal
		    // has been decided and should be acted upon. 
		    parent.decided(set);
		}
	    }
	    else {
		// This value belongs in a new set. Just need to
		// re-propose this value .
		rePropose(proposal);
	    }
	}

	/**
	 * Received Phase 2B message from an Acceptor. Once the Commander has
	 * received enough messages from the Acceptors for a particular slot,
	 * we can decide the slot. 
	 */
	private  void receiveP2B(Acceptor acceptor, 
				Ballot ballot, 
				Proposal proposal) {
	    // Check if this is the same ballot we had sent out. 
	    if(ballot.compare(ballotNum) == Ballot.SAME) {
		// Is this reply for a classic or fast slot?
		PaxosCommunicator.Quorum quorumType = getQuorumPolicy(proposal);
		// logger.log(Level.INFO, String.format("%s: received P2B from %s (%s) (%s) (%s)", 
		// 				     getID(), 
		// 				     acceptor.getID(), 
		// 				     proposal.toString(),
		// 				     Arrays.toString(proposal.getValue()),
		// 				     quorumType)); 

		// Check if we have already decided this slot.
		if(alreadyDecided(proposal)) {
		    // If this is a classic value, then there's nothing else
		    // to do and we can drop this message. Most likely what happened
		    // was that we accepted the message with 2/3 majority and this is
		    // simply the remaining confirmation. 
		    if(quorumType == PaxosCommunicator.Quorum.FAST) {
			// This is a fast slot. There are two possibilities to consider:
			// (1) This is a conflicting confirmation. In this case, we should
			// re-propose this value in classic mode. 
			// (2) This is a remaining confirmation. In this case, we should
			// ignore this confirmation. 
			if(isConflicting(ballot, proposal)) {
			    rePropose(proposal);
			}
			// else {
			    // logger.log(Level.INFO, String.format("%s: ignoring remaining confirmation %s", 
			    // 					 getID(), proposal));

			    // for(String a : completedSlots.getAuthors()) {
			    // 	logger.log(Level.INFO, String.format("%s: confirmed authors %s", 
			    // 					     getID(), a));
				
			    // }
			// }
		    }
		    return;
		}

		// We have not decided this slot yet. We must check if there a quorum
		// of values for this slot. This is tricky since we may receive values
		// for slots out of order.
		if(quorumType == PaxosCommunicator.Quorum.SIMPLE) {
		    // For classic mode, it is pretty simple. we just need to keep track
		    // of the acceptors that have sent replies and determine when we have
		    // enough to make a quorum. 
		    handleP2BClassic(acceptor, ballot, proposal);
		}
		else {
		    // For fast mode, it is less straightforward. We need to group the
		    // proposals into commutative sets. Within a single set, we wait until
		    // there is a quorum value for each distinct source before applying. 
		    // All other sets get re-proposed. 
		    handleP2BFast(acceptor, ballot, proposal);
		}
	    }
	    else {
		// Cancel the timer. It's possible to receive P2B messages
		// even when a P2A message hasn't been sent (another Proposer 
		// may send the message). 
		synchronized(this) {
		    if(timer != null) {		    
			timer.cancel();
		    }
		}

		// Not the same ballot. Must be a higher ballot
		// number. Send a pre-empted message to the 
		// leader with the new ballot number.
		parent.preempted(ballot, proposal);
	    }
	}
    }

    /**
     * Compare two proposers. Proposers must have a global order
     * for the ballot exchange protocol.
     *
     * @param proposer Proposer to compare against.
     * @return Negative value if this proposer is "less", 0 if the "same",
     * and positive value otherwise. 
     */
    public int compare(Proposer proposer) {
	int c = super.getID().compareTo(proposer.getID());
	
	if(c == 0)     { return Ballot.SAME; }
	else if(c < 0) { return Ballot.LESS; }
	else           { return Ballot.GREATER; }
    }

    /**
     * Handle recovery messages.
     */
    private boolean handleRecoveryMessage(PaxosRole node,
					  PaxosMessage msg) {
	// Check if any of these are recovery messages. If so,
	// process them even if we are still in diagnostic mode. 
	if(msg.getState() == PaxosState.Phase.RECOVERA) {
	    // Received a message from the network to recover this
	    // Proposer from the recovery log.
	    proprec.recover();
	    return true;
	}
	else if(msg.getState() == PaxosState.Phase.RECOVERB) {
	    // The replica has sent a reply containing the
	    // last completed slot made by the replica. 
	    List<PaxosValue> values = msg.getPayload();

	    // Deserialize the complete slot value. 
	    PrimitivePaxosValue sv = 
	    	PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));
	    proprec.receiveCompleteSlot((Replica)node, sv.intValue());


	    // // Check if we need to initiate fast mode. 
	    // PrimitivePaxosValue fs = 
	    // 	PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(1));
	    // PrimitivePaxosValue fe = 
	    // 	PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(2));
	    // if(fs.intValue() != 0 && fe.intValue() != 0) {
	    // 	fastMode((Replica)node, 
	    // 		 fs.intValue(), 
	    // 		 fe.intValue()); 
	    // }

	    return true;
	}

	return false;
    }

    /**
     * Receive a message from the network. 
     * This method just dispatches to more specific handlers. 
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

	if(msg.getState() == PaxosState.Phase.ELECT) {	    
	    logger.log(Level.INFO, 
		       String.format("%s: received ELECT from %s", getID(), node.getType()));

	    // Received a message from the network (most likely some
	    // sort of election protocol) to make this Proposer the
	    // "leader". The protocol should operate correctly even if
	    // multiple Proposers think they are leaders, but this simplifies things. 
	    if(!leader && !inDiagnostic()) { 
		setLeader(true);
		start(); // Start the Scout phase. 
	    }
	}
	else if(msg.getState() == PaxosState.Phase.DEMOTE) {	    
	    // Received a message from the network that this Proposer is
	    // no longer a leader. Just set the leader bit to false. 
	    setLeader(false);
	}
	else if(msg.getState() == PaxosState.Phase.PB) { 
	    // Received a message from an Acceptor accepting our Phase 1A
	    // message. Once we've received enough of these messages we can
	    // on to Phase 2 of the protocol. 
	    List<PaxosValue> values = msg.getPayload();

	    // First value specifies the number of total PB
	    // messages to expect. 
	    PrimitivePaxosValue num = 
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));

	    // Next value should be the ballot.
	    Ballot b = PrimitivePaxosValue.deSerializeBallot(values.get(1));

	    synchronized(this) {
		// Once we receive all the PB messages, we can collate the proposals. 
		List<PaxosMessage> replies = pbReplies.get(b);
		if(replies == null) {
		    replies = new ArrayList<PaxosMessage>();
		    pbReplies.put(b, replies);
		}

		if(num.intValue() == 1 ||
		   num.intValue() - 1 == pbReplies.get(b).size()) {

		    // Collate all the proposals. 
		    List<Proposal> allProposals = new ArrayList<Proposal>();
		    replies.add(msg);

		    for(PaxosMessage r : replies) {
			values = r.getPayload();

			for(int i = 2; i < values.size(); ++i) {
			    Proposal proposal = PrimitivePaxosValue.deSerializeProposal(values.get(i));
			    allProposals.add(proposal);
			}
		    }

		    // Make sure we have a scout that actually sent
		    // out the original Phase 1 request. Otherwise we
		    // should ignore this "reply". 
		    if(scout != null) {
			scout.receiveP1B((Acceptor)node, b, allProposals);
		    }

		    // Clear the message queue. 
		    pbReplies.remove(b);
		}
		else {
		    // Add the message to the list of replies. Organize
		    // replies by the ballot. 
		    replies.add(msg);
		}
	    }
	}
	else if(msg.getState() == PaxosState.Phase.CB) {
	    // Received a message from an Acceptor accepting our Phase 2A
	    // message. Once we've received enough of these messages we can
	    // decide on the proposal. 
	    List<PaxosValue> values = msg.getPayload();

	    // Get the parameters for this message.
	    Ballot ballot = 
		PrimitivePaxosValue.deSerializeBallot(values.get(0));
	    Proposal prop =
		PrimitivePaxosValue.deSerializeProposal(values.get(1));

	    // Get the associated commander. Since this may be for
	    // a fast slot, we need to use ranges.
	    Commander cmd = getCommander(prop, true);
	    if(cmd != null) {
		cmd.receiveP2B((Acceptor)node, ballot, prop);
	    }
	    else {
		// This may be for an orphaned fast proposal. This happens when a client 
		// submits a fast proposal using an old Proposer's ballot after a 
		// new Proposer has already assumed control.
		if(active &&
		   getQuorumPolicy(prop) == PaxosCommunicator.Quorum.FAST) {
		    // Make sure that we have not already tried starting this one. 
		    if(proposals.get(prop.getSlot()) == null) {
			logger.log(Level.INFO, String.format("%s: re-submitting orphan proposal %s (from:%s) (our ballot: %s)", 
							     getID(), 
							     prop.toString(), 
							     node.getID(),
							     ballotNum.toString()));

			// Re-submit the value. 	
			prop.setEndSlot(Integer.MIN_VALUE);
			proposals.put(prop.getSlot(), prop);
			startCommander(ballotNum, prop, getQuorumPolicy(prop), false);
		    }
		}
	    }
	}
	else if(msg.getState() == PaxosState.Phase.FASTSLOTS) {
	    // Received a messsage from a Replica to initiate fast mode.
	    List<PaxosValue> values = msg.getPayload();

	    // Deserialize the primitive value.
	    PrimitivePaxosValue sv =
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(0));
	    PrimitivePaxosValue ev =
		PrimitivePaxosValue.deSerializePrimitive((PrimitivePaxosValue)values.get(1));

	    // Initiate fast mode. 
	    fastMode((Replica)node, 
		     sv.intValue(), 
		     ev.intValue()); 
	}
	else if(msg.getState() == PaxosState.Phase.PROPOSE) {
	    // Received a message from a Replica to propose a new message. 
	    // This will initiate the two-phase messaging process. 
	    List<PaxosValue> values = msg.getPayload();

	    Proposal proposal = PrimitivePaxosValue.deSerializeProposal(values.get(0));
	    propose(proposal, false);
	}
    }

    /**
     * Perform recovery for Proposers. 
     */
    class ProposerRecovery extends PaxosRecovery {
	private Proposer proposer;
	private volatile boolean recoveredSlots;

	public ProposerRecovery(PaxosRole r) {
	    proposer = (Proposer)r;
	    recoveredSlots = true;
	}

	/**
	 * Send a message to a Replica to retrieve the latest completed slot.
	 */
	private void recoverCompleteSlot() {
	    PaxosMessage payload = new PaxosMessage();
	    payload.setState(PaxosState.Phase.RECOVERP);
	    payload.setSender(proposer);
	    
	    recoveredSlots = false;
	    proposer.send(PaxosRole.Role.REPLICA, payload);
	}

	/**
	 * Received a reply from a Replica containing the latest complete slot. 
	 */
	public void receiveCompleteSlot(Replica replica, Integer value) {
	    logger.log(Level.INFO, String.format("%s: received completed slot %d from %s", 
						 proposer.getID(), value, replica.getID()));

	    completedSlots.add(value.intValue(), null);
	    recoveredSlots = true;
	}

	/**
	 * Recover proposals from the local log.
	 **/
	private String recoverFromLog() {
	    // Now open up an iterator.
	    LogMessage log = null;
	    List<PaxosMessage> recover = new ArrayList<PaxosMessage>();
	    PaxosStorageIterator iter;

	    for(iter = storage.playback(); iter.hasNext(); ) {
		try {
		    log = iter.next();
		} catch(PaxosStorageException e) {
		    // The log message was corrupted. Stop the recovery and throw
		    // an exception. 
		    e.printStackTrace();
		}

		if(Arrays.equals(log.getID(), "ballot".getBytes())) {
		    // Recover the ballot number. 
		    Ballot ballot = new Ballot();
		    ballot.unSerialize(log.getMsg());
		    proposer.setBallot(ballot);
		}
	    }

	    // Tell the storage how much data we've already
	    // read. This will help us during append operations. 
	    storage.setAppendStart(iter.getReadBytes());

	    // Return the name of the log file used during recovery. 
	    return storage.getLogFile();
	}

	/**
	 * Recover this proposer from a crash.
	 */
	public synchronized void recover() {
	    logger.log(Level.INFO, String.format("%s: recovering state", proposer.getID()));
	    // Reset all the datastructures.
	    proposer.init();

	    // Set the diagnostic bit. 
	    proposer.setDiagnostic(true);

	    // Recover the active proposals. 
	    String logFile = recoverFromLog();

	    // Tell the proposer to start a new log. 
	    proposer.reuseLog(logFile);

	    // Fetch the latest complete slot. 
	    recoverCompleteSlot();

	    // Wait to make sure that we have actually recovered
	    // the complete slot value. 
	    while(!recoveredSlots) {
		try {Thread.sleep(64); }
		catch(InterruptedException e) { }
	    }

	    // This ends the recovery process.
	    proposer.setDiagnostic(false);
	}
    }
}
