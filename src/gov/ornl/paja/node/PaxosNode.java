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

package gov.ornl.paja.node;

/**
 * Java libs.
 */ 
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.net.InetSocketAddress;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Configuration libs.
 */
import gov.ornl.config.ConfigFactory;
import gov.ornl.config.Configuration;
import gov.ornl.config.ConfigEntry;

/**
 * Paxos libs.
 **/
import gov.ornl.paja.proto.PaxosState;
import gov.ornl.paja.proto.PaxosStateMachine;
import gov.ornl.paja.proto.PaxosStateMachineFactory;
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PrimitivePaxosValue;
import gov.ornl.paja.roles.Replica;
import gov.ornl.paja.roles.Proposer;
import gov.ornl.paja.roles.Acceptor;
import gov.ornl.paja.roles.PaxosRole;
import gov.ornl.paja.roles.DiagnosticRole;
import gov.ornl.paja.roles.PaxosMessage;
import gov.ornl.paja.roles.PaxosCommunicator;
import gov.ornl.paja.network.netty.NettyCommunicator;
import gov.ornl.paja.network.netty.StaticNetwork;

/**
 * A Paxos node is a container for several of the Paxos roles. 
 * Specifically it contains a single instance of a Replica, Proposer, and
 * Acceptor. Roles within a node communicate via a virtual interface, while
 * roles across nodes communicate over a network. Most of the time, clients
 * will interact directly with a node instead of individual roles. 
 *
 * @author James Horey
 */
public class PaxosNode {
    /**
     * General debug logger. 
     */
    private static Logger logger = 
	Logger.getLogger("PaxosNode"); 

    /**
     * Unique ID of this Paxos node.
     */
    public String id;

    /**
     * ID of the current leader.
     */
    private String leader;

    /**
     * Way to communicate with other nodes. 
     */
    private InternalCommunicator com;

    /**
     * The diagnosis mode is used to indicate whether this group is
     * being used for diagnosis/debugging purposes. When it is in diagnostic
     * mode, the multicast operation will be sent to *ALL* nodes. 
     */
    private boolean diagnosisMode = false;

    /**
     * Keep track of whether node is up or not. 
     */
    private boolean active;

    /**
     * Different roles that co-habitate a single node.
     */
    private Acceptor acceptor;
    private Replica replica;
    private Proposer proposer;

    /**
     * Keep a handle to the state machine so that we can
     * restart replicas properly. 
     */
    private PaxosStateMachineFactory stateMachineFactory = null;

    /**
     * Where the recovery logs are stored. 
     */
    private String logDir;

    /**
     * Where the network information is stored.
     */
    private String networkDir;
    
    /**
     * Filename of the configuration.
     */
    private String configFile;
    private ConfigFactory configFactory;

    /**
     * Create an uninitialized Paxos node. 
     */
    public PaxosNode() {
	configFile = null;
	leader = null;
    }

    /**
     * @param configFile Configuration file defining network, etc. 
     * @param recover Indicate whether this node should perform a
     * recovery from the logs. 
     */
    public PaxosNode(String configFile) { 
	this.configFile = configFile;
	this.leader = null;

	// Parse the configuration file. 
	parseConfig();

	// Create an internal communicator. 
	com = new InternalCommunicator();
    }

    /**
     * Set the diagnosis mode. 
     */
    public void setDiagnosisMode(boolean diagnosisMode) {
	this.diagnosisMode = diagnosisMode;
    }

    /**
     * Start the Paxos node. 
     */
    public void start() {
	// Start listening for connections.
	active = true;

	// Start listening on the network. 
	com.startListening();

	// Create the roles. This assumes that the configuration
	// has been parsed, etc. 
	createRoles(false);
    }

    /**
     * Restart the Paxos node. 
     */
    public void restart() {
	if(!active) {
	    // Reset the active bit. 
	    active = true;

	    // Just create new acceptors, etc. and tell them to 
	    // recover from the logs. 
	    createRoles(true);

	    // Make sure that we know who the leader is.
	    setLeader(com.getNetwork().getLeader());
	}
    }

    /**
     * Stop the node. 
     */
    public void stop() {
	active = false;

	// Stop the various roles.
	acceptor.setDiagnostic(true);
	replica.setDiagnostic(true);
	proposer.setDiagnostic(true);
	proposer.stopScout();
	proposer.stopCommanders();
    }

    /**
     * Create and initialize the roles. 
     */
    private void createRoles(boolean recover) {
	PaxosStateMachine sm = null;
	if(stateMachineFactory != null) {
	    try {
		sm = stateMachineFactory.newStateMachine();
	    } catch(Exception e) {
		e.printStackTrace();
	    }
	}

	acceptor = new Acceptor(id, logDir, recover);
	replica = new Replica(id, logDir, recover, sm);
	proposer = new Proposer(id, logDir, recover);

	acceptor.setDiagnoser(new DiagnosticRole(acceptor));
	replica.setDiagnoser(new DiagnosticRole(replica));
	proposer.setDiagnoser(new DiagnosticRole(proposer));

	acceptor.setCommunicator(com);
	replica.setCommunicator(com);
	proposer.setCommunicator(com);

	if(recover) {
	    acceptor.recover();
	    replica.recover();
	    proposer.recover();
	}
    }

    /**
     * Parse the configuration file to get information
     * regarding this node. 
     */
    private void parseConfig() {
	Configuration conf;
	ConfigEntry entry;

	configFactory = new ConfigFactory();
	conf = configFactory.getConfig(Paths.get(configFile).toAbsolutePath().toString());
	if(conf != null) {
	    entry = conf.get("paja.id");
	    id = entry.getEntry("value").getValues().get(0);

	    entry = conf.get("paja.log.dir");
	    logDir = entry.getEntry("value").getValues().get(0);

	    entry = conf.get("paja.network.dir");
	    networkDir = entry.getEntry("value").getValues().get(0);
	}
    }

    /**
     * Each node has a unique ID that is recursively
     * assigned to all the different roles. 
     */
    public void setID(String id) {
	this.id = id;
    }
    public String getID() {
	return id;
    }

    /**
     * The log directory is where all the recovery logs go. 
     */
    public void setLogDir(String dir) {
	logDir = dir;
    }
    public String getLogDir() {
	return logDir;
    }

    /**
     * The network directory is where all the node network information
     * is stored. Each node has its own separate file. 
     */
    public void setNetworkDir(String dir) {
	networkDir = dir;
    }
    public String getNetworkDir() {
	return networkDir;
    }

    /**
     * Get the network used by this group. This is not very
     * modular right now, so we will need to address this in the future.
     */
    public StaticNetwork getNetwork() {
	return com.getNetwork();
    }

    /**
     * Get/set the state machine. 
     **/
    public void setStateMachineFactory(PaxosStateMachineFactory f) {
	stateMachineFactory = f;
    }

    /**
     * Set the fast window. 
     */
    public void updateFastWindow(int fastWindowSize) {
	// Construct the election message.
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.FASTSLOTS);
	msg.setSender(proposer);
	msg.addPayload(new PrimitivePaxosValue(fastWindowSize));

	// Send the message to the proposer.
	com.send(replica, msg);
    }

    /**
     * ID of the current leader.
     */
    public void setLeader(String leader) {
	// Was there an old leader? 
	if(leader != null) {
	    demoteProposer();
	}

	// Switch the leadership. 
	this.leader = leader;

	// This node has been elected as leader. 
	// We must inform the internal Proposer. 
	if(leader.equals(id)) {
	    electProposer();
	}
    }
    public String getLeader() {
	return leader;
    }

    /**
     * Inform the Proposer role that this node has been elected
     * leader. This will initiate Phase 1 of the consensus protocol. 
     */
    private void electProposer() {
	// Construct the election message.
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.ELECT);
	msg.setSender(proposer);

	// Send the message to the proposer.
	com.send(proposer, msg);
    }

    /**
     * Inform the Proposer role that this node has been demoted and
     * should no longer consider itself the leader. 
     */
    private void demoteProposer() {
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.DEMOTE);
	msg.setSender(proposer);

	// Send the message to the proposer.
	com.send(proposer, msg);
    }
    
    /**
     * Internal communication channel. That way when a role
     * sends a message to other roles, it will either direct it
     * internally or direct it externally to other nodes. 
     */
    class InternalCommunicator extends PaxosCommunicator {
	private NettyCommunicator externalCom;
	private StaticNetwork network;

	public InternalCommunicator() {
	    // Initialize the network.
	    network = new StaticNetwork(networkDir);

	    // Initialize the external communiciation. 
	    externalCom = new NettyCommunicator(network);

	    // Set ourselves as the receiving parent of all
	    // communication from the external network. 
	    externalCom.addUpstream(this);
	}

	/**
	 * Get the network associated with this communicator. 
	 */
	public StaticNetwork getNetwork() {
	    return network;
	}

	/**
	 * Get list of acceptors for the Proposers.
	 */
	@Override public List<PaxosRole> getAcceptors() {
	    return network.getAcceptors();
	}

	/**
	 * Get the leader associated with this communicator. 
	 */
	public String getLeader() {
	    return network.getLeader();
	}

	/**
	 * Start listening for network communication. 
	 */
	protected void startListening() {
	    externalCom.startListening(id);
	}

	/**
	 * Stop the server. 
	 */
	protected void stopListening() {
	    externalCom.stopListening();
	    externalCom.removeUpstream(this);
	}

	/**
	 * Retrieve one of the internal Paxos roles. 
	 */
	private PaxosRole getRole(PaxosRole.Role role) {
	    if(role == PaxosRole.Role.ACCEPTOR) {
		return acceptor;
	    }
	    else if(role == PaxosRole.Role.REPLICA) {
		return replica;
	    }
	    else if(role == PaxosRole.Role.PROPOSER) {
		return proposer;
	    }

	    return null;
	}

	/**
	 * Get the leader node. 
	 */
	public PaxosRole getDesirable(PaxosRole.Role role) {
	    String leader = network.getLeader();
	    return PaxosRole.newRole(role, leader);
	}

	/**
	 * Send a message to a non-specific ode with the specified role.
	 * Usually this means sending to a local role on the same node,
	 * unless the sending role is already on the node. 
	 */
	public void send(PaxosRole sender, PaxosRole.Role role, PaxosMessage msg) {
	    if(sender.getType() == role) {
		// Send to an external. 
		externalCom.send(sender, role, (PaxosMessage)msg.copy());
	    }
	    else {
		// Send to an internal. 
		getRole(role).receiveMessage(msg.getSender(), 
					     (PaxosMessage)msg.copy());
	    }
	}

	/**
	 * Send a specific message to the supplied role. 
	 */
	public void send(PaxosRole destination, PaxosMessage msg) {
	    if(destination == acceptor ||
	       destination == replica ||
	       destination == proposer) {

		// This is an internal message. 
		destination.receiveMessage(msg.getSender(), 
					   (PaxosMessage)msg.copy());
	    }
	    else {
		// This is an external message. 
		externalCom.send(destination, 
				 (PaxosMessage)msg.copy());
	    }
	}

	/**
	 * Multi-cast a message to all nodes with a specific role. 
	 */
	public int multicast(PaxosRole.Role role, PaxosMessage msg, Quorum policy) {
	    // Get the number of nodes necessary for a quorum. 
	    int expected = getQuorum(network.getNumNodes(), policy);

	    // Send one of the messages to our local role.
	    send(getRole(role), msg);

	    if(diagnosisMode) {
		// Send to all the nodes in diagnosis mode. 
		externalCom.multicast(role, 
				      (PaxosMessage)msg.copy(),
				      Quorum.ALL);
	    }
	    else {
		// Send the remaining to the external network. 
		externalCom.multicast(role, 
				      (PaxosMessage)msg.copy(),
				      expected - 1);
	    }

	    return SUCCESS;
	}

	/**
	 * A message has been successfully received. 
	 */
	public void updateCallback(PaxosMessage msg) {
	    if(msg.getState() == PaxosState.Phase.CRASH) {
		// The network is asking this node to stop.
		stop();
	    }
	    else if(msg.getState() == PaxosState.Phase.REBOOT) {
		// Restart the node. 
		restart();
	    }
	    else if(msg.getState() == PaxosState.Phase.PRINT) {
		// The network is asking this node to print out state. 
		replica.printStateMachine();
	    }
	    else {
		// Check which role this message is intended for. 
		PaxosRole.Role role = msg.getDestination();
		PaxosRole node = getRole(role);

		// Deliver the message. 
		if(node != null) {
		    // Update the routing table.
		    if(msg.getRemoteAddress() != null) {
			network.updateRoute(msg.getSender().getID(),
					    msg.getRemoteAddress(), 
					    msg.getRemotePort());
		    }

		    // Inform the node of the new message.
		    node.receiveMessage(msg.getSender(), msg);
		}
	    }
	}

	/**
	 * A message delivery has failed. 
	 */
	public void failureCallback(PaxosMessage msg) {
	    // Don't do anything for now. 
	}
    }
}