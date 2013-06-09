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
import java.util.Collection;
import java.nio.ByteBuffer;

/**
 * A ballot is a tuple <leader, number> used during the Paxos protocol.
 * Ballots are unbounded and are globally ordered. So, for example, a leader
 * should be able to increment ballots to compete with other leaders, etc. 
 *
 * To implement this, we assume that Proposers also have a global order. 
 * First we should compare the ballot number. Think of this as a "round". If
 * two proposals are in the same round, then we just use the Proposer order.
 * Otherwise, the greater round wins. 
 * 
 * Examples:
 * (0, 0) < (1, 0)
 * (0, 1) > (1, 0)
 * (0, 1) < (1, 1)
 * (0, 2) > (1, 1)
 *
 * @author James Horey
 */
public class Ballot implements PaxosValue {
    /**
     * Comparison values for ballots. 
     */
    public static final int GREATER  = 1;
    public static final int LESS     = -1;
    public static final int SAME     = 0;

    private String proposer;
    private long value;

    public Ballot() {
	proposer = null;
	value = Long.MIN_VALUE;
    }

    /**
     * @param proposer Identity of the proposer that owns this ballot. 
     */
    public Ballot(String proposer) {
	this.proposer = proposer;
	value = 0;
    }

    /**
     * Compare the two ballots. 
     * 
     * @param ballot Ballot to compare against
     */
    public int compare(Ballot ballot) {
	long v = ballot.getBallotValue();

	if(v == value) {
	    int c = proposer.compareTo(ballot.getProposer());
	
	    if(c == 0)     { return SAME; }
	    else if(c < 0) { return LESS; }
	    else           { return GREATER; }
	}
	else if(v < value) {
	    return GREATER;
	}
	else {
	    return LESS;
	}
    }

    /**
     * Increment the ballot. 
     *
     * @param ballot Ballot to increment
     */
    public void increment(Ballot ballot) {
	value = ballot.getBallotValue() + 1;
    }

    /**
     * Get the round value. Used to compare ballots.
     **/
    protected void setBallotValue(long v) {
	value = v;
    }
    protected long getBallotValue() {
	return value;
    }

    /**
     * Return the byte[] version of the value.
     *
     * @return Actual byte-array value
     */
    @Override public byte[] getValue() {
	return null;
    }

    /**
     * Get the collection of values.
     *
     * @return List of all the values
     */
    @Override public Collection<byte[]> getValues() {
	return null;
    }

    /**
     * Get all the issuers associated with this value. 
     *
     * @return List of all the "issuers". 
     */
    @Override public Collection<String> getIssuers() {
	return null;
    }

    /**
     * Get the proposer. Used to compare ballots.
     *
     * @return ID of the original proposer
     */
    protected String getProposer() {
	return proposer;
    }

    /**
     * Serialize the ballot to a byte[]. 
     *
     * @return Serialized value
     */
    public byte[] serialize() {
	byte[] p = proposer.getBytes();
	ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / 8 + p.length);

	buf.putLong(value);
	buf.put(p);

	return buf.array();
    }

    /**
     * Initialize the ballot from the byte[]. 
     *
     * @param Serialized value
     */
    public void unSerialize(byte[] data) {
	if(data != null) {
	    ByteBuffer buf = ByteBuffer.wrap(data);
	    int longSize = Long.SIZE / 8;

	    value = buf.getLong();
	    proposer = new String(data, longSize , data.length - longSize);
	}
    }

    /**
     * Make a copy of the data (useful for debugging).
     *
     * @return Copy of the value
     */
    public PaxosValue copy() {
	Ballot b = new Ballot(new String(proposer));
	b.setBallotValue(value);

	return b;
    }

    /**
     * Determine if the object is equivalent to this ballot.
     *
     * @param obj Object to compare equality
     * @return Positive if this value is greater, zero if
     * the same, negative otherwise. 
     */
    public boolean equals(Object obj) {
	Ballot b = (Ballot)obj;

	return 
	    value == b.getBallotValue() &&
	    proposer.equals(b.getProposer());
    }

    /**
     * Get the hashcode of this object. 
     *
     * @return Hash code value
     */
    public int hashCode() {
	return (int)value * proposer.hashCode();
    }

    /**
     * Get the string representation of this ballot.
     *
     * @param String representation
     */
    public String toString() {
	return String.format("<%s,%d>", proposer, value);
    }

    /**
     * Get/set the slot. Not really applicable for a ballot.
     * Just to include to make it a proper PaxosValue.
     **/
    @Override public void setSlot(int s) {
    }
    @Override public int getSlot() {
	return -1;
    }
}