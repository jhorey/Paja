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
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.net.ServerSocket;
import java.net.SocketAddress;
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
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PrimitivePaxosValue;
import gov.ornl.paja.roles.Client;
import gov.ornl.paja.roles.Replica;
import gov.ornl.paja.roles.PaxosRole;
import gov.ornl.paja.roles.PaxosMessage;
import gov.ornl.paja.roles.PaxosCommunicator;
import gov.ornl.paja.network.netty.NettyCommunicator;
import gov.ornl.paja.network.netty.StaticNetwork;

/**
 * A Paxos group is the "front end" to interact with a set of
 * Paxos nodes. The interface is meant to be simple and encapsulate
 * key consensus protocol features. 
 *
 * @author James Horey
 */
public class PaxosGroup {
    /**
     * Paxos client interface. 
     */
    private Client client;

    /**
     * Used to control when to re-submit a value after a failure.
     */
    private static final int MIN_RESUBMIT_PAUSE = 256;
    private volatile int numFailures = 0;

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
     * Current view of the network and routing information. 
     */
    private StaticNetwork network;

    /**
     * Port that the client listens on.
     */
    private int clientPort;

    /**
     * Unique ID of this Paxos node.
     */
    public String id;

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
     * Generate some random values. 
     */
    private static Random rand = new Random(System.currentTimeMillis());

    /**
     * Log all errors, info, etc. 
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosGroup"); 

    /**
     * @param configFile Configuration file defining network, etc. 
     * @param recover Indicate whether this node should perform a
     * recovery from the logs. 
     */
    public PaxosGroup(String configFile) { 
	this.configFile = configFile;

	// Parse the configuration file. 
	parseConfig();

	// Create a network and communicator.
	com = new InternalCommunicator();

	// Create a client role.
	client = new Client(id, logDir);
	client.setCommunicator(com);
    }

    /**
     * Set the client ID. 
     */
    public void setClientID(String id) {
	client.setID(id);
    }

    /**
     * Set the diagnosis mode. 
     */
    public void setDiagnosisMode(boolean diagnosisMode) {
	this.diagnosisMode = diagnosisMode;
    }

    /**
     * This "starts" the group processes. 
     */
    public void connect() {
	// Tell the client to listen on an empty port,
	// and update the network. 
	clientPort = com.startListening();
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
     * Each client has a unique ID. 
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
     * Propose a value to the consensus group. 
     *
     * @param Value that is being proposed. 
     */
    public void propose(byte[] value) {
	// Construct a propose message.
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.REQUEST);
	msg.setSender(client);
	msg.setRemotePort(clientPort);
	msg.addPayload(new PrimitivePaxosValue(value));

	// We need to send a message to the leader. 
	// This is really just a shell of a role (it doesn't
	// do anything other than hold the ID). 
	PaxosRole leadNode = new Replica(com.getLeader(), null, false, null);

	// Use the client to send the message to a Replica. 
	client.send(leadNode, msg);
    }

    /**
     * Learn the agreed upon value for the given slot.
     *
     * @param slot The slot of the value.
     * @return Value agreed agreed upon for the slot. The value may be
     * null if the slot has not been agreed upon yet. 
     */
    public byte[] learn(int slot) {
	return null;
    }

    /**
     * Learn the maximum agreed upon slot value. 
     *
     * @return Last slot containing a fully agreed upon value. 
     */
    public int maxCompleteSlot() {
	return 0;
    }

    /**
     * Internal communication channel. 
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
	 * Find a free port for this client. 
	 */
	private int findFreePort() {
	    int port = 0;

	    try {
		ServerSocket socket = new ServerSocket(0);
		port = socket.getLocalPort();
		socket.close();
	    } catch(IOException e) {
		e.printStackTrace();
	    }

	    return port;
	}

	/**
	 * Start listening for network communication. 
	 */
	protected int startListening() {
	    // Find a free port and update the routing table. 
	    int port = findFreePort();
	    network.updateRoute(id, "localhost", port);
	    
	    // Now start listening for messages. 
	    externalCom.startListening(id);

	    return port;
	}

	/**
	 * Send a message to a non-specific ode with the specified role.
	 * Usually this means sending to a local role on the same node,
	 * unless the sending role is already on the node. 
	 */
	public void send(PaxosRole sender, PaxosRole.Role role, PaxosMessage msg) {
	    // Pause a short time before sending to reduce congestion. 
	    try { Thread.sleep(getSendWindow()); }
	    catch(InterruptedException e) { e.printStackTrace(); }
				      
	    externalCom.send(sender, role, (PaxosMessage)msg.copy());
	}

	/**
	 * Send a specific message to the supplied role. 
	 */
	public void send(PaxosRole destination, PaxosMessage msg) {
	    // Pause a short time before sending to reduce congestion. 
	    try { Thread.sleep(getSendWindow()); }
	    catch(InterruptedException e) { e.printStackTrace(); }

	    externalCom.send(destination, (PaxosMessage)msg.copy());
	}

	/**
	 * Multi-cast a message to all nodes with a specific role. 
	 */
	public int multicast(PaxosRole.Role role, PaxosMessage msg, Quorum policy) {
	    // Pause a short time before sending to reduce congestion. 
	    try { Thread.sleep(getSendWindow()); }
	    catch(InterruptedException e) { e.printStackTrace(); }

	    if(diagnosisMode) {
		return externalCom.multicast(role, (PaxosMessage)msg.copy(), Quorum.ALL);
	    }
	    else {
		return externalCom.multicast(role, (PaxosMessage)msg.copy(), policy);
	    }
	}

	/**
	 * Update the congestion window and return the amount
	 * of time to wait before re-submitting. 
	 */
	private int updateResubmitWindow(boolean success) {
	    if(success && numFailures > 0) {
		numFailures--;
	    }
	    else if(!success) {
		numFailures++;
	    }

	    // Calculate time to sleep on log scale. 
	    return 
		MIN_RESUBMIT_PAUSE + 
		100 * numFailures;
	}

	/**
	 * Get the amount of time to wait before transmitting. 
	 * This will help during high congestion periods. 
	 */
	private int getSendWindow() {
	    return 
		rand.nextInt(8) + 
		5 * numFailures;
	}

	/**
	 * A message has been successfully received. 
	 */
	public void updateCallback(PaxosMessage msg) {
	    // Check which role this message is intended for. 
	    PaxosRole.Role role = msg.getDestination();

	    // Deliver the message. 
	    if(role == PaxosRole.Role.CLIENT) {
		client.receiveMessage(msg.getSender(), msg);
	    }

	    // Update the congestion window.
	    updateResubmitWindow(true);
	}

	/**
	 * A message delivery has failed. 
	 */
	public void failureCallback(PaxosMessage msg) {
	    // This is an error message. For whatever reason
	    // the proposal was not submitted successfully. 
	    // Instead of immediately re-submitting, wait a small
	    // amount of time first. 
	    int time = updateResubmitWindow(false);

	    // System.out.printf("failure message %s (%d %d)\n", msg, time, numFailures);
	    try { Thread.sleep(time); }
	    catch(InterruptedException e) { e.printStackTrace(); }

	    PrimitivePaxosValue value = (PrimitivePaxosValue)msg.getPayload().get(0);
	    propose(value.arrayValue());
	}
    }
}