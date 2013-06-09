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
import gov.ornl.paja.proto.PaxosState;
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.PrimitivePaxosValue;

/**
 * Java libs.
 **/
import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;

/**
 * This class is used to contain all the state and data for message transmissions.
 * This includes the actual values (usually proposals), sender/destination information,
 * and protocol state. 
 *
 * @author James Horey
 */
public class PaxosMessage {
    /**
     * Maximum number of proposals that can fit into a single message. 
     */
    public static final int MAX_PROPOSALS = 8;

    /**
     * Remote address and port information. Used only for
     * external messages. 
     */
    private String remoteAddress;
    private int remotePort;

    /**
     * The destination role. This is not a specific process
     * but any process with the specified role. 
     */
    private PaxosRole.Role destination;

    /**
     * Specific process that has sent this message.
     */
    private PaxosRole sender;

    /**
     * List of values associated with this message. Usually
     * proposals or commutative sets. 
     */
    private List<PaxosValue> values;

    /**
     * Paxos state for this message.
     */
    private PaxosState.Phase state;

    /**
     * Instantiate an empty message. 
     */
    public PaxosMessage() {
	remoteAddress = null;
	remotePort = -1;
	sender = null;
	destination = null;
	values = new ArrayList<>();
    }

    /**
     * Get/set the sender.
     */
    public void setSender(PaxosRole sender) {
	this.sender = sender;
    }
    public PaxosRole getSender() {
	return sender;
    }

    /**
     * Get/set the destination.
     */
    public void setDestination(PaxosRole.Role role) {
    	destination = role;
    }
    public PaxosRole.Role getDestination() {
    	return destination;
    }

    /**
     * Get the value associated with this message.
     */
    public void addPayload(PaxosValue value) {
	values.add(value);
    }
    public List<PaxosValue> getPayload() {
	return values;
    }

    /**
     * The remote address is used to keep track of the connection
     * used by the sender of the message. 
     */
    public void setRemoteAddress(String addr) {
	remoteAddress = addr;
    }
    public String getRemoteAddress() {
	return remoteAddress;
    }

    /**
     * Remote port is used to keep track of the sender's open port.
     * We do this assuming that the sender needs to transmit this info. 
     */
    public void setRemotePort(int port) {
	remotePort = port;
    }
    public int getRemotePort() {
	return remotePort;
    }

    /**
     * The Paxos state indicates where in the state machine
     * this message was sent. 
     */
    public void setState(PaxosState.Phase state) {
	this.state = state;
    }
    public PaxosState.Phase getState() {
	return state;
    }

    /**
     * Serialize the message to a byte array.
     *
     * @return Serialized message
     */
    public byte[] serialize() {
	int serializedSize = 0;
	List<byte[]> valueBuffers = new ArrayList<>();
	ByteBuffer buffer = null;

	// Serialize each value.
	for(PaxosValue v : values) {
	    byte[] valueBuffer = v.serialize();
	    serializedSize += (Integer.SIZE / 8) + valueBuffer.length;
	    valueBuffers.add(valueBuffer);
	}

	// Serialize the sender.
	byte[] idBuffer = sender.getID().getBytes();
	serializedSize += (Integer.SIZE / 8) + idBuffer.length;

	// Increase the size for number of values,
	// state ordinal, destination ordinal, sender ordinal,
	// and finally the remote port.
	serializedSize += 5 * (Integer.SIZE / 8);

	// Allocate the byte buffer and pack all the data into a buffer. 
	// First pack the serialized values. 
	buffer = ByteBuffer.allocate(serializedSize);
	buffer.putInt(values.size());
	for(byte[] value : valueBuffers) {
	    buffer.putInt(value.length);
	    buffer.put(value);
	}

	// Pack the Paxos state.
	buffer.putInt(state.ordinal());

	// Pack the destination role.
	buffer.putInt(destination.ordinal());

	// Pack the sender.
	buffer.putInt(idBuffer.length);
	buffer.put(idBuffer);
	buffer.putInt(sender.getType().ordinal());

	// Pack the remote port. 
	buffer.putInt(remotePort);

	buffer.flip();
	return buffer.array();
    }

    /**
     * Initialize the message from a byte array.
     *
     * @param data Serialized message. 
     */
    public void unSerialize(byte[] data) {
	ByteBuffer buffer = ByteBuffer.wrap(data);
	int numValues = 0;
	
	values.clear();
	numValues = buffer.getInt();
	for(int i = 0; i < numValues; ++i) {
	    int bufLength = buffer.getInt();
	    byte[] buf = new byte[bufLength];
	    
	    buffer.get(buf);
	    PrimitivePaxosValue v = new PrimitivePaxosValue(buf);
	    v.setType(PrimitivePaxosValue.SERIALIZED);
	    values.add(v);
	}

	// Unpack the Paxos state. 
	int ordinal = buffer.getInt();
	state = PaxosState.Phase.values()[ordinal];

	// Unpack the destination.
	ordinal = buffer.getInt();
	destination = PaxosRole.Role.values()[ordinal];

	// Unpack the sender ID
	int bufLength = buffer.getInt();
	byte[] idBuffer = new byte[bufLength];
	buffer.get(idBuffer);
	String id = new String(idBuffer);

	// Unpack the sender type
	ordinal = buffer.getInt();
	sender = PaxosRole.newRole(PaxosRole.Role.values()[ordinal], id);

	// Unpack the remote port.
	remotePort = buffer.getInt();
    }

    /**
     * Make a copy of the message.
     *
     * @return New message copy
     */
    public PaxosMessage copy() {
	PaxosMessage msg = new PaxosMessage();

	msg.setSender(sender);
	msg.setDestination(destination);
	msg.setState(state);
	msg.setRemotePort(remotePort);
	msg.setRemoteAddress(remoteAddress);

	for(PaxosValue v : values) {
	    if(v == null) {
		System.out.printf("null payload!\n");
	    }
	    msg.addPayload(v.copy());
	}

	return msg;
    }

    /**
     * Construct an error message.
     *
     * @param Error description
     * @return Error message 
     */
    public static PaxosMessage ERROR_MSG(String msg) {
	PaxosMessage error = new PaxosMessage();
	error.addPayload(new PrimitivePaxosValue(msg.getBytes()));

	return error;
    }
}