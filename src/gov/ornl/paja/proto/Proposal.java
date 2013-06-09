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

package gov.ornl.paja.proto;

/**
 * Java libs.
 **/
import java.util.Arrays;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.nio.ByteBuffer;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A proposal. A proposal consists of the following elements:
 * (1) A value to be decided
 * (2) A slot indicating the "order" in which the value should be applied
 * (3) A ballot assigned by a Proposer
 * Optionally ballots may be a "ranged" ballot so that it is used
 * for multiple slots (used in fast mode). 
 * 
 * @author James Horey
 */ 
public class Proposal implements Comparable<Proposal>, PaxosValue {
    private int slot;      // Start slot. 
    private int endSlot;   // Optional end slot.
    private Ballot ballot; // Ballot of this proposal.
    private String issuer; // ID of issuing client. 
    private byte[] value;  // The value associated with the proposal

    /**
     * Log all the errors, warnings, and messages.
     */
    protected static Logger logger = 
	Logger.getLogger("Proposal"); 

    /**
     * Create a completely empty Proposal that must be initialized
     * via unserialization.
     */
    public Proposal() {
	slot = Integer.MIN_VALUE;
	endSlot = Integer.MIN_VALUE;
	ballot = null;
	issuer = null;
	value = null;
    }

    /**
     * @param id The unique ID of this proposal 
     */
    public Proposal(String id) {
	slot = Integer.MIN_VALUE;
	endSlot = Integer.MIN_VALUE;
	ballot = null;
	issuer = id;
	value = null;
    }

    /**
     * @param id The unique ID of this proposal 
     * @param slot Slot to use
     * @param ballot Ballot to use in this proposal
     * @param value Value to use in this proposal
     */
    public Proposal(String id, int slot, Ballot ballot, byte[] value) {
	this.slot = slot;
	endSlot = Integer.MIN_VALUE;
	this.ballot = ballot;
	this.value = value;
	issuer = id;
    }

    /**
     * Create a ranged Proposal.
     *
     * @param id The unique ID of this proposal 
     * @param slot Starting slot to use
     * @param slot Ending slot to use
     * @param ballot Ballot to use in this proposal
     * @param value Value to use in this proposal
     */
    public Proposal(String id, int slot, int endSlot, Ballot ballot, byte[] value) {
	this.slot = slot;
	this.endSlot = endSlot;
	this.ballot = ballot;
	this.value = value;
	issuer = id;
    }

    /**
     * Get/set the issuing client ID.
     */
    public void setIssuer(String i) {
	issuer = i;
    }
    public String getIssuer() {
	return issuer;
    }

    /**
     * Get all the issuers associated with this proposal. For
     * a normal proposal, there is only one issuer. 
     *
     * @return List of all the "issuers". 
     */
    @Override public Collection<String> getIssuers() {
	List<String> collection = 
	    new ArrayList<>(1);
	collection.add(issuer);

	return collection;
    }

    /**
     * Get the slot. 
     */
    @Override public void setSlot(int slot) {
	this.slot = slot;
    }
    @Override public int getSlot() {
	return slot;
    }

    /**
     * Get the ending slot. This is used when we need to send
     * the same value for multiple slots.
     */
    public void setEndSlot(int endSlot) {
	this.endSlot = endSlot;
    }
    public int getEndSlot() {
	return endSlot;
    }

    /**
     * Get/set the ballot.
     */
    public void setBallot(Ballot ballot) {
	this.ballot = ballot;
    }
    public Ballot getBallot() {
	return ballot;
    }

    /**
     * Get/set the proposal value.
     */
    public void setValue(byte[] value) {
	this.value = value;
    }
    @Override public byte[] getValue() {
	return value;
    }

    /**
     * Get the collection of values.
     *
     * @return List of all the values
     */
    @Override public Collection<byte[]> getValues() {
	Collection<byte[]> values = 
	    new ArrayList<byte[]>(1);

	values.add(value);

	return values;
    }

    /**
     * Serialize the proposal to a byte[]. 
     *
     * @return Serialized value
     */
    @Override public byte[] serialize() {
	int intSize = (6 * Integer.SIZE) / 8;
	int issuerLength = 0;
	int ballotLength = 0;
	byte[] issuerBuf = null;
	byte[] ballotBuf = null;

	// The Proposal may not yet have a ballot assigned.
	if(ballot != null) {
	    ballotBuf = ballot.serialize();
	    ballotLength = ballotBuf.length;
	}

	if(issuer != null) {
	    issuerBuf = issuer.getBytes();
	    issuerLength = issuerBuf.length;
	}

	// Allocate a byte buffer.
	ByteBuffer buf = ByteBuffer.allocate(intSize + 
					     ballotLength + 
					     value.length + 
					     issuerLength);

	// Store all the lengths. 
	buf.putInt(slot);
	buf.putInt(endSlot);
	buf.putInt(value.length);
	buf.putInt(ballotLength);
	buf.putInt(issuerLength);

	// Place the value. 
	buf.put(value);
	if(ballotLength > 0) {
	    buf.put(ballotBuf);
	}

	if(issuerLength > 0) {
	    buf.put(issuerBuf);
	}

	return buf.array();
    }

    /**
     * Initialize the proposal from the byte[]. 
     *
     * @param Serialized value
     */
    @Override public void unSerialize(byte[] data) {
	ByteBuffer buf = ByteBuffer.wrap(data);

	slot = buf.getInt();
	endSlot = buf.getInt();
	int valueLength = buf.getInt();
	int ballotLength = buf.getInt();
	int issuerLength = buf.getInt();

	// Read in the value. 
	value = new byte[valueLength];
	buf.get(value);

	// Read in the ballot information.
	if(ballotLength > 0) {
	    byte[] ballotBuf = new byte[ballotLength];
	    buf.get(ballotBuf);

	    // Unserialize the ballot.
	    ballot = new Ballot();
	    ballot.unSerialize(ballotBuf);
	}

	// Read the issuer.
	if(issuerLength > 0) {
	    byte[] i = new byte[issuerLength];
	    buf.get(i);
	    issuer = new String(i);
	}
    }

    /**
     * Make a copy of the data (useful for debugging).
     *
     * @return Copy of the value
     */
    @Override public PaxosValue copy() {
	byte[] v = null;
	if(value != null) {
	    v = Arrays.copyOf(value, value.length);
	}

	Ballot b = null;
	if(ballot != null) {
	   b = (Ballot)ballot.copy();
	}

	return new Proposal(new String(issuer), slot, endSlot, b, v);
    }

    /**
     * Determine if the object is equivalent to this proposal.
     * Proposal are equivalent if they have the same slot,
     * issuer, and value. 
     *
     * @param obj Object to compare equality
     * @return Positive if this value is greater, zero if
     * the same, negative otherwise. 
     */
    @Override public boolean equals(Object obj) {
	Proposal p = (Proposal)obj;
	
	if(this == p) {
	    return true;
	}

	// Check the slot
	if(slot != p.getSlot()) {
	    return false;
	}

	// Check if proposal is issued by the same client. 
	if(issuer != null &&
	   p.getIssuer() != null) {
	    if(!issuer.equals(p.getIssuer())) {
		return false;
	    }
	}

	// Check the value. 
	if(value != null && p.getValue() != null) {
	    return Arrays.equals(value, p.getValue());	    
	}

	return false;
    }

    /**
     * Get the hashcode of this object. 
     *
     * @return Hash code value
     */
    public int hashCode() {
	if(ballot == null) {
	    return slot;
	}

	return slot ^ ballot.hashCode();
    }

    /**
     * Get the string representation of this proposal.
     *
     * @param String representation
     */
    public String toString() {
	String msg = String.format("slot:%d ", slot);

	if(ballot != null) {
	    msg += String.format("ballot:%s ", ballot.toString());
	}

	if(issuer != null) {
	    msg += String.format("issuer:%s ", issuer.toString());
	}

	return msg;
    }

    /**
     * For Comparable interface.
     */
    @Override public int compareTo(Proposal p) {
	if(ballot == null ||
	   p.getBallot() == null) {
	    return slot - p.getSlot();
	}

	int c = ballot.compare(p.getBallot());
	if(c == Ballot.LESS) {
	    return -1;
	}
	else if(c == Ballot.GREATER) {
	    return 1;
	}
	else {
	    return slot - p.getSlot();
	}
    }

    /**
     * Create a simple NOOP proposal.
     */
    public static Proposal NOOP(String id, int slot, Ballot ballot) {
	return new Proposal(id, slot, ballot, null);
    }
}