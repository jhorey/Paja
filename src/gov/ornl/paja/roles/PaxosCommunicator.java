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
 * Paxos libs
 **/
import gov.ornl.paja.proto.*;

/**
 * Java libs.
 **/
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * Abstract class for a Paxos communicator. This class handles all
 * the communication for the Paxos roles. There may be different 
 * communicator implementations (TCP, shared memory, etc.). 
 * 
 * @author James Horey
 */
public class PaxosCommunicator {
    /**
     * Type of quorum necessary for a particular message. 
     * SIMPLE is a 2/3 majority, FAST is a 3/4 majority,
     * ALL requires all the nodes.
     */
    public enum Quorum {
	SIMPLE, FAST, ALL
    }

    /**
     * Return codes used during multicast/send. 
    **/
    public static final int NO_QUORUM    = 0;
    public static final int NO_PREFERRED = 1;
    public static final int SUCCESS      = 2;

    /**
     * Preferred nodes associated with a particular role. Used when
     * a message targets a particular role, and we need to decide which
     * process should receive the message.
     */
    protected Map<PaxosRole.Role, List<PaxosRole>> preferred = 
	new HashMap<>();

    /**
     * Add to the set of preferred quorum nodes. These nodes
     * should be used preferentially to construct a quorum. 
     *
     * @param node Paxos process to add
     */
    public void addPreferred(PaxosRole node) {
	List<PaxosRole> nodes = preferred.get(node.getType());

	if(nodes == null) {
	    nodes = new ArrayList<>();
	    preferred.put(node.getType(), nodes);
	}

	nodes.add(node);
    }

    /**
     * Get the list of nodes that are preferentially associated
     * with the supplied role.
     *
     * @param role Paxos role
     * @return List of nodes associated with the role. 
     */
    public List<PaxosRole> getPreferred(PaxosRole.Role role) {
	return preferred.get(role);
    }

    /**
     * Get the number of nodes that constitute a quorum. 
     *
     * @param total Total number of nodes to consider
     * @param policy Quorum policy
     * @return Number of nodes in the quorum
     */
    public static int getQuorum(int total, Quorum policy) {
	int expected = total;
	int failures = 0;

	if(policy == Quorum.SIMPLE) {
	    failures = (int)Math.ceil((float)(total - 1) / 2.0);
	    expected = failures + 1;
	}
	else if(policy == Quorum.FAST) {
	    failures = (int)Math.ceil((float)(total - 1) / 3.0);
	    expected = 2 * failures + 1;
	}

	return expected;
    }

    /**
     * Indicate whether we have a quorum. This is based off of the
     * the quorum policy. 
     * 
     * @param size Number of nodes to consider
     * @param total Total number of nodes to consider
     * @param policy Quorum policy
     * @return Indicate SUCCESS or NO_QUORUM
     */
    public static int isQuorum(int size, int total, Quorum policy) {
	int expected = getQuorum(total, policy);

	if(size >= expected) {
	    return SUCCESS;
	}
	else {
	    return NO_QUORUM;
	}
    }

    /**
     * Get the leader node. 
     * Override for specific communicators. 
     *
     * @param role Paxos role
     * @return Leader node associated with the role. 
     */
    public PaxosRole getDesirable(PaxosRole.Role role) {
	return null;
    }

    /**
     * Send a message to a random node with the specified role.
     * Override for specific communicators. 
     *
     * @param sender Node sending the message
     * @param role Destination role
     * @param msg Message to send
     */
    public void send(PaxosRole sender, PaxosRole.Role role, PaxosMessage msg) {
    }

    /**
     * Multi-cast a message to all nodes with a specific role. 
     * Override for specific communicators. 
     *
     * @param role Destination role
     * @param msg Message to send
     * @param policy Quorum policy
     * @return Success code
     */
    public int multicast(PaxosRole.Role role, PaxosMessage msg, Quorum policy) {
	return SUCCESS;
    }

    /**
     * Send a specific message to the supplied role. 
     * Override for specific communicators. 
     *
     * @param destination Destination role
     * @param msg Message to send
     */
    public void send(PaxosRole destination, PaxosMessage msg) {
    }

    /**
     * Callback to indicate a message has been successfully received. 
     * Override for specific communicators. 
     *
     * @param msg Message received
     */
    public void updateCallback(PaxosMessage msg) {
    }

    /**
     * Callback to indicate a message delivery has failed.
     * Override for specific communicators. 
     *
     * @param msg Message received 
     */
    public void failureCallback(PaxosMessage msg) {
	// In the future, we would dequeue the msg and 
	// contact the sending role. The PaxosRole error
	// handling must be defined (to contact another server, etc)...
	System.out.printf("comm: msg send failed\n");
    }

    /**
     * Get list of acceptors for the Proposers. Used during
     * quorum testing. 
     *
     * @return List of acceptors
     */
    public List<PaxosRole> getAcceptors() {
	return null;
    }

    /**
     * Add an "upstream" communicator. This communicator gets all
     * success/failure messages. 
     *
     * @param com Upstream communicator
     */
    public void addUpstream(PaxosCommunicator com) {
    }

    /**
     * Remove an "upstream" communicator.
     *
     * @param com Upstream communicator
     */
    public void removeUpstream(PaxosCommunicator com) {
    }
}