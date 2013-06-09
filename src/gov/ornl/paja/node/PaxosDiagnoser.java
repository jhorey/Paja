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
import java.util.Map;
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
public class PaxosDiagnoser {
    /**
     * Way to communicate with other nodes. 
     */
    private InternalCommunicator com;
    private Client client;

    /**
     * Current view of the network and routing information. 
     */
    private StaticNetwork network;

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
     * Log all errors, info, etc. 
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosDiagnoser"); 

    /**
     * @param configFile Configuration file defining network, etc. 
     * @param recover Indicate whether this node should perform a
     * recovery from the logs. 
     */
    public PaxosDiagnoser(String configFile) { 
	this.configFile = configFile;

	// Parse the configuration file. 
	parseConfig();

	// Create a network and communicator.
	com = new InternalCommunicator();

	// Create a client role.
	client = new Client("diagnoser", logDir);
	client.setCommunicator(com);
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
	    entry = conf.get("paja.log.dir");
	    logDir = entry.getEntry("value").getValues().get(0);

	    entry = conf.get("paja.network.dir");
	    networkDir = entry.getEntry("value").getValues().get(0);
	}
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
     * Print out the state machines. 
     */
    public void printState() {
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.PRINT);
	msg.setSender(client);

	// Ask all the Replicas. 
	client.multicast(PaxosRole.Role.REPLICA, msg, PaxosCommunicator.Quorum.ALL);
    }

    /**
     * Crash the majority of nodes. 
     */
    public void stopMajority(boolean leader) {
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.CRASH);
	msg.setSender(client);

	com.excludeLeader(leader);
	client.multicast(PaxosRole.Role.REPLICA, msg, 
			 PaxosCommunicator.Quorum.SIMPLE);
    }

    /**
     * Crash just a minority of nodes
     */
    public void stopMinority(boolean leader) {
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.CRASH);
	msg.setSender(client);

	com.excludeLeader(leader);
	client.send(PaxosRole.Role.REPLICA, msg);
    }

    /**
     * Recover all crashed nodes. 
     */
    public void recover() {
	PaxosMessage msg = new PaxosMessage();
	msg.setState(PaxosState.Phase.REBOOT);
	msg.setSender(client);

	// Send to all the nodes. The ones that are
	// still up will just ignore the message.
	client.multicast(PaxosRole.Role.REPLICA, msg, 
			 PaxosCommunicator.Quorum.ALL);	
    }

    /**
     * Internal communication channel. 
     */
    class InternalCommunicator extends PaxosCommunicator {
	private NettyCommunicator externalCom;
	private StaticNetwork network;
	private boolean leaderExclude = false;
	
	public InternalCommunicator() {
	    // Initialize the network.
	    network = new StaticNetwork(networkDir);

	    // Initialize the external communiciation. 
	    externalCom = new NettyCommunicator(network);
	    
	    // Set ourselves as the receiving parent of all
	    // communication from the external network. 
	    externalCom.addUpstream(this);
	}

	public void excludeLeader(boolean exclude) {
	    leaderExclude = exclude;
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

	private PaxosRole getAvailable(String exclude, PaxosRole.Role role) {
	    Map<String, StaticNetwork.Route> routes = network.getAllRoutes();

	    for(String id : routes.keySet()) {
		if(!id.equals(exclude)) {
		    return PaxosRole.newRole(role, id);
		}
	    }

	    return null;
	}

	/**
	 * Send a message to a non-specific ode with the specified role.
	 * Usually this means sending to a local role on the same node,
	 * unless the sending role is already on the node. 
	 */
	public void send(PaxosRole sender, PaxosRole.Role role, PaxosMessage msg) {
	    String leader = getLeader();
	    if(leaderExclude) {
		// Choose a random, non-leader node. 
		PaxosRole destination = getAvailable(leader, role);
		externalCom.send(destination, msg);
	    }
	    else {
		// Choose the leader. 
		externalCom.send(PaxosRole.newRole(role, leader), msg);
	    }
	}

	/**
	 * Send a specific message to the supplied role. 
	 */
	public void send(PaxosRole destination, PaxosMessage msg) {
	    msg.setDestination(destination.getType());
	    externalCom.send(destination, msg);
	}

	/**
	 * Multi-cast a message to all nodes with a specific role. 
	 */
	public int multicast(PaxosRole.Role role, PaxosMessage msg, Quorum policy) {
	    // Get the number of nodes necessary for a quorum. 
	    int expected = getQuorum(network.getNumNodes(), policy);
	    Map<String, StaticNetwork.Route> routes = network.getAllRoutes();

	    // Might need to exclude the leader. 
	    String leader = null;
	    if(leaderExclude) {
		leader = getLeader();
	    }

	    int i = 0;
	    for(String id : routes.keySet()) {
		if(leader == null ||
		   !id.equals(leader)) {

		    send(PaxosRole.newRole(role, id), msg);
		    ++i;
		}

		if(i == expected) {
		    break;
		}
	    }

	    return PaxosCommunicator.SUCCESS;
	}

	/**
	 * A message has been successfully received. 
	 */
	public void updateCallback(PaxosMessage msg) {
	}

	/**
	 * A message delivery has failed. 
	 */
	public void failureCallback(PaxosMessage msg) {
	}
    }
}