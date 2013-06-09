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
import gov.ornl.paja.proto.PaxosState;

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
 * Abstract class for all Paxos roles. This includes Clients, Replicas,
 * Proposers, and Acceptors. 
 *
 * @author James Horey
 */
public abstract class PaxosRole {
    /**
     * Paxos role types.
     **/
    public static enum Role {
	CLIENT,
	    REPLICA,
	    PROPOSER,
	    ACCEPTOR,
	    UNKNOWN
    };

    // Log all the errors, warnings, and messages.
    protected static Logger logger = 
	Logger.getLogger("PaxosRole"); 

    /**
     * Where we store the recovery logs. All Paxos nodes
     * record their state in the recovery logs. 
     */
    private String logDir;

    /**
     * Unique ID associated with this Paxos node. 
     */
    private String id;

    /**
     * The "type" of this node (Client, Acceptor, etc.)
     */
    private Role roleType;

    /**
     * Way to communicate with other nodes. 
     */
    protected PaxosCommunicator comm;

    /**
     * Handle to the node-specific logging & recovery mechanism.
     */
    protected PaxosRecovery recovery;

    /**
     * The diagnostic node is used to handle debugging &
     * diagnostic handling (printing state, etc.).
     */
    private DiagnosticRole dr;

    /**
     * Indicate whether this node is in a repair/diagnostic
     * state. If it is, it will not receive any normal messages. 
     */
    protected volatile boolean diagnosticMode;

    /**
     * @param id Unique ID of this role 
     * @param logDir Where the logs are contained
     */
    public PaxosRole(String id, String logDir) {
	this.id = id;
	this.logDir = logDir;
	comm = null;
	roleType = Role.UNKNOWN;
	recovery = null;
	diagnosticMode = false;
	dr = null;
    }

    /**
     * Initialize the role. 
     * Should be overriden by specific node types.
     */
    protected abstract void init();

    /**
     * Perform recovery. 
     */
    public void recover() {
	recovery.setLogFile(String.format("%s/%s", logDir, getType()));
	recovery.recover();
    }

    /**
     * Start a new log. 
     */
    public void startNewLog() {
	recovery.setLogFile(String.format("%s/%s/%s", 
					  logDir, 
					  getType(), 
					  getLogID()));
	recovery.startNewLog();
    }

    /**
     * Re-use an existing log file. 
     *
     * @param fileName Name of the existing log file
     */
    public void reuseLog(String fileName) {
	recovery.setLogFile(fileName);
	recovery.reuseLog();
    }

    /**
     * Get/set the recovery mechanism. 
     */
    public void setRecovery(PaxosRecovery recovery) {
	this.recovery = recovery;
    }
    public PaxosRecovery getRecovery() {
	return recovery;
    }

    /**
     * Set a diagnosis object that helps diagnose this node
     * during operation. This is optional for production runs.
     */
    public void setDiagnoser(DiagnosticRole dr) {
	// Add the diagnosis states.
	if(dr != null) {
	    this.dr = dr;
	}
    }
    public DiagnosticRole getDiagnoser() {
	return dr;
    }

    /**
     * Indicate whether this proposer is in a repair/diagnostic
     * state. If it is, it will not respond to normal messages. 
     *
     * @param mode Diagnostic mode
     */
    public void setDiagnostic(boolean mode) {
	diagnosticMode = mode;
    }
    public boolean inDiagnostic() {
	return diagnosticMode;
    }

    /**
     * Get/set the log directory. This is where all the logs are
     * stored for each epoch value. 
     *
     * @param logDir Where we store the recovery logs.
     */ 
    public void setLogDir(String logDir) {
	this.logDir = logDir;
    }
    public String getLogDir() {
	return logDir;
    }

    /**
     * Get/set the role type.
     * 
     * @param role The Paxos role type
     */
    public void setType(Role role) {
	roleType = role;
    }
    public Role getType() {
	return roleType;
    }

    /**
     * Get/set the ID for this role.
     */
    public void setID(String id) {
	this.id = id;
    }
    public String getID()  {
	return id;
    }

    /**
     * Return the log ID. These log IDs should be unique. 
     *
     * @return Unique Log ID. 
     */
    public String getLogID() {
	// Use a combination of the the ID and current time.  
	return String.format("logfile%s-%s-%s", 
			     getID(), roleType, System.currentTimeMillis());
    }

    /**
     * Get/set the communicator. Responsible for communicating with
     * the other roles. 
     *
     * @param comm The Paxos communicator 
     */
    public void setCommunicator(PaxosCommunicator comm) {
	this.comm = comm;
    }
    public PaxosCommunicator getCommunicator() {
	return comm;
    }

    /**
     * Send a message to a single member of the role. 
     * Used during recovery, etc. 
     *
     * @param role The node to send the message to
     * @param msg The message to send
     */
    public void send(PaxosRole.Role role,
		     PaxosMessage msg) {
	if(comm != null) {
	    comm.send(this, role, msg);
	}
    }

    /**
     * Multicast the message over the network using
     * a specific quorum policy.
     *
     * @param role The node to send the message to
     * @param msg The message to send
     * @param quorum Quorum policy
     */
    public void multicast(PaxosRole.Role role,
			  PaxosMessage msg,
			  PaxosCommunicator.Quorum quorum) {
	if(comm != null) {
	    comm.multicast(role, msg, quorum);
	}
	else {
	    logger.log(Level.WARNING, 
		       String.format("the communication mechanism not set"));
	}
    }

    /**
     * Multicast the message over the network using the
     * set quorum policy. 
     *
     * @param role The node to send the message to
     * @param msg The message to send
     */
    public void multicast(PaxosRole.Role role,
			  PaxosMessage msg) {
	if(comm != null) {
	    comm.multicast(role, msg, PaxosCommunicator.Quorum.ALL);
	}
    }

    /**
     * Send the message over the network to specific node. 
     *
     * @param node Destination node
     * @param msg The message to send
     */
    public void send(PaxosRole node,
		     PaxosMessage msg) {
	if(comm != null) {
	    msg.setSender(this);
	    comm.send(node, msg);
	}
    }

    /**
     * Receive a message from the network. 
     *
     * @param node Sending node
     * @param msg The message to send
     */
    public void receiveMessage(PaxosRole node,
			       PaxosMessage msg) {
    }

    /**
     * Determine if the supplied object is "equal". This method
     * just uses the unique ID associated with the node. 
     *
     * @param Object we are comparing against
     * @param true if objects are equal. 
     */
    public boolean equals(Object obj) {
	PaxosRole a = (PaxosRole)obj;

	if(this == a) {
	    return true;
	}
	else {
	    // Use the ID string. 
	    return a.getID().equals(getID());
	}
    }

    /**
     * Generate a hash code for this object.
     *
     * @return Hash code value
     */
    public int hashCode() {
	// Use the ID string. 
	return getID().hashCode();
    }

    /**
     * Instantiate a new Paxos node. The Paxos node will be
     * "empty" and most likely used for messaging purposes. 
     *
     * @param role The Paxos role for this node
     * @param id The unique ID for this node
     * @return New Paxos node
     */
    public static PaxosRole newRole(Role role, String id) {
	switch(role) {
	case CLIENT:
	    return new Client(id, null);
	case REPLICA:
	    return new Replica(id, null, false, null);
	case PROPOSER:
	    return new Proposer(id, null, false);
	case ACCEPTOR:
	    return new Acceptor(id, null, false);
	default:
	    return null;
	}
    }
}