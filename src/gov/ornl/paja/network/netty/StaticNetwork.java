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

package gov.ornl.paja.network.netty;

/**
 * Java libs.
 */
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.net.InetSocketAddress;

/**
 * Configuration libs.
 */
import gov.ornl.config.ConfigFactory;
import gov.ornl.config.Configuration;
import gov.ornl.config.ConfigEntry;

/**
 * Paxos libs.
 */ 
import gov.ornl.paja.roles.PaxosRole;
import gov.ornl.paja.roles.Acceptor;

/**
 * Encapsulates a view of the network. This means keeping track of which 
 * nodes are online and information (IP, port) necessary to contact a node. 
 * It is a static view in the sense that network information is captured from 
 * a configuration file. That means if a node's IP addresses changes, or a 
 * new node is added, the entire network will have to be re-started. 
 *
 * @author James Horey
 */
public class StaticNetwork {
    /**
     * Where all the network configuration files live.
     */
    private String networkDir;
    private ConfigFactory configFactory;

    /**
     * Our routing table.
     */
    private NavigableMap<String, Route> routing;

    /**
     * Number of data nodes.
     */ 
    private int numNodes;

    /**
     * Designated leader. 
     */
    private String leader = null;

    public StaticNetwork(String networkDir) {
	this.networkDir = networkDir;
	routing = new ConcurrentSkipListMap<>();
	numNodes = 0;

	// The network directory should include a set of 
	// files. Each file should describe a single node. 
	try {
	    DirectoryStream<Path> stream = 
		Files.newDirectoryStream(Paths.get(networkDir).toAbsolutePath());

	    for(Path entry : stream) {
		parseConfig(entry);
	    }

	} catch(Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * Parse the configuration file to get information
     * regarding this network. 
     */
    private void parseConfig(Path configFile) {
	Configuration conf;
	ConfigEntry entry;

	configFactory = new ConfigFactory();
	conf = configFactory.getConfig(configFile.toString());
	if(conf != null) {
	    Route route = new Route();

	    entry = conf.get("network.id");
	    String id = entry.getEntry("value").getValues().get(0);

	    entry = conf.get("network.ip");
	    route.ip = entry.getEntry("value").getValues().get(0);

	    entry = conf.get("network.port");
	    route.port = Integer.parseInt(entry.getEntry("value").getValues().get(0));

	    // Store in the routing table. 
	    routing.put(id, route);
	    numNodes++;
	}
    }

    /**
     * Fetch the route for a particular node.
     *
     * @param node ID identifying the node. 
     * @return A route for the node. 
     */
    public Route getRoute(String node) {
	return routing.get(node);
    }

    /**
     * Fetch a desirable route for a particular role.
     *
     * @param exclude Do not include the supplied node
     * @return A route for the node. 
     */
    public Route getDesirable(String exclude) {
	// Are any of the nodes marked as leaders? 
	if(leader != null) {
	    return routing.get(leader);
	}
	else {
	    // Pick a random node.
	    for(String id : routing.keySet()) {
		if(!id.equals(exclude)) {
		    return routing.get(id);
		}
	    }

	    return null;
	}
    }

    /**
     * Get all the routes. 
     *
     * @return Map of all routes. The key is the unique
     * ID of a node, and then values are the actual routes. 
     */
    public Map<String, Route> getAllRoutes() {
	return routing;
    }

    /**
     * Identify if the ID belongs to a client. 
     *
     * Right now just check the first part of the ID. 
     * There should be a better way of differentiating clients
     * from other nodes. 
     *
     * @param id ID of the client
     * @return True if the ID is associated with a client 
     */
    public boolean isClientConnection(String id) {
	return id.charAt(0) == 'c';
    }

    /**
     * Get all the acceptors.
     * 
     * @return List of all the acceptors 
     */
    public List<PaxosRole> getAcceptors() {
	List<PaxosRole> acceptors = 
	    new ArrayList<PaxosRole>();

	for(String id : routing.keySet()) {
	    if(!isClientConnection(id)) {
		acceptors.add(new Acceptor(id, null, false));
	    }
	}

	return acceptors;
    }

    /**
     * Assign a leader in the network. The leader is responsible
     * for the actual submission, etc. 
     *
     * @param id The ID of the leader. 
     */
    public void setLeader(String id) {
	leader = id;
    }
    public String getLeader() {
	return leader;
    }

    /**
     * Elect one of the nodes to be the leader. 
     */
    public void electLeader() {
	// Choose the node with the lowest ID. 
	setLeader(routing.firstKey());
    }

    /**
     * Get the number of nodes in the network.
     *
     * @return Number of nodes
     */
    public int getNumNodes() {
	return numNodes;
    }

    /**
     * Check if a client has disconnected so that we can
     * clean up the routing table. 
     *
     * @param addr Address to be updated
     * @param port Port to be updated
     * @return True if the node was remoted. False otherwise.
     */
    public boolean updateDisconnected(String addr, int port) {
	String disconnect = null;

	// Find the disconnected route and confirm
	// that it is a client route. 
	for(String id : routing.keySet()) {
	    Route route = routing.get(id);

	    if(isClientConnection(id)) {
		if(route.port == port &&
		   route.ip.equals(addr)) {
		    disconnect = id;
		    break;
		}
	    }
	}

	// Now disconnect the route. 
	if(disconnect != null) {
	    routing.remove(disconnect);
	    return true;
	}

	return false;
    }

    /**
     * Update the routing table. This does not increase the number
     * of reported nodes in the network since we assume that the
     * new route is for a client. 
     *
     * @param id ID of the node to update
     * @param addr Address to be updated
     * @param port Port to be updated
     */
    public void updateRoute(String id, String addr, int port) {
	Route route = routing.get(id);
	if(route == null) {
	    // This is a brand new role, so create a new 
	    // route for this role. 
	    route = new Route();
	    route.ip = addr;
	    route.port = port;
	    routing.put(id, route);
	}
	else if(port != -1) {
	    if(route.port != port ||
	       !route.ip.equals(addr)) {
		// Check if the address or port has changed. This
		// might happen if we a role has restarted with new
		// address information so it still retains the same ID. 
		route.port = port;
		route.ip = addr;
	    }
	}
    }

    /**
     * Organize the IP and port. 
     */
    public class Route {
	public String ip;
	public int port;
    }
}