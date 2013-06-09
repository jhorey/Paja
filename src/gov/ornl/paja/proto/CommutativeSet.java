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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;

/**
 * A commutative set consists of proposals/value that belong in a single
 * equivalence class. Proposals within a single set can be applied in
 * any order by the state machines. This is used to implement "general"
 * Paxos where we may see multiple proposals submitted for a single slot. 
 *
 * @author James Horey
 */ 
public class CommutativeSet implements PaxosValue {
    private Map<Proposal, Set<String>> set;
    private Set<String> issuers;
    private int total;
    private int slot;

    public CommutativeSet() {
	set = new ConcurrentHashMap<>();
	issuers = new HashSet<>();
	total = -1;
	slot = 0;
    }

    /**
     * Indicate whether the set is empty. 
     * 
     * @return True if empty. False otherwise. 
     */
    public boolean isEmpty() {
	return set.size() == 0;
    }

    /**
     * Add a value to this set. 
     *
     * @param proposal Proposal to add to the set
     * @param id ID of the acceptor that accepted the proposal
     */
    public void add(Proposal proposal, String id) {
	Set<String> acceptors = set.get(proposal);

	if(acceptors == null) {
	    acceptors = new HashSet<>();
	    set.put(proposal, acceptors);
	}

	acceptors.add(id);
	issuers.add(proposal.getIssuer());
    }

    /**
     * Get total number of acceptors across all values.
     */
    public int totalAcceptors() {
	if(total == -1) {
	    total = 0;

	    for(Proposal proposal : set.keySet()) {
		total += getNumAcceptors(proposal);
	    }
	}

	return total;
    }

    /**
     * Get the number of proposals associated with this value.
     */
    public int getNumAcceptors(Proposal proposal) {
	Set<String> acceptors = set.get(proposal);

	if(acceptors != null) {
	    return acceptors.size();
	}
	else {
	    return 0;
	}
    }

    /**
     * Get the collection of values.
     */
    public Collection<Proposal> getProposals() {
	Collection<Proposal> values = 
	    new ArrayList<>(set.size());

	for(Proposal p : set.keySet()) {
	    values.add(p);
	}

	return values;
    }

    /**
     * Get a single byte buffer for the values. 
     *
     * @return Actual byte-array value
     */
    @Override public byte[] getValue() {
	return null;
    }

    /**
     * Get the list of values.
     *
     * @return List of all the values
     */
    @Override public Collection<byte[]> getValues() {
	List<byte[]> values = 
	    new ArrayList<>();

	for(Proposal p : set.keySet()) {
	    values.add(p.getValue());
	}

	return values;
    }

    /**
     * Get the issuers associated with all the values in the set. 
     *
     * @return List of all the "issuers". 
     */
    @Override public Collection<String> getIssuers() {
	return issuers;
    }

    /**
     * Serialize the value. We do not fully serialize the
     * commutative set, since we only need the values to be
     * transmitted to the replicas. 
     *
     * @return Serialized value
     */
    @Override public byte[] serialize() {
	int size = (3 * Integer.SIZE) / 8;

	// Calculate the size of the set. 
	List<byte[]> serializedProposals = 
	    new ArrayList<>();
	for(Proposal p : set.keySet()) {
	    byte[] pa = p.serialize();
	    size += (Integer.SIZE / 8 ) + pa.length;
	    serializedProposals.add(pa);
	}

	// Now begin serializing. 
	ByteBuffer buf = ByteBuffer.allocate(size);
	buf.putInt(slot); // Slot
	buf.putInt(totalAcceptors()); // Total number of acceptors. 
	buf.putInt(set.size()); // Number of distinct values. 

	for(byte[] pa : serializedProposals) {
	    buf.putInt(pa.length);
	    buf.put(pa);
	}

	return buf.array();
    }

    /**
     * Instantiate from a byte array.
     *
     * @param Serialized value
     */
    @Override public void unSerialize(byte[] data) {
	ByteBuffer buf = ByteBuffer.wrap(data);
	slot = buf.getInt();
	total = buf.getInt();
	int numKeys = buf.getInt();

	for(int i = 0; i < numKeys; ++i) {
	    int valLength = buf.getInt();
	    byte[] valBuf = new byte[valLength];
	    buf.get(valBuf);

	    // The commutative set does not actually need
	    // to store the proposals. Just the values & count. 
	    Proposal proposal = new Proposal();
	    proposal.unSerialize(valBuf);
	    Set<String> emptySet = new HashSet<>();
	    set.put(proposal, emptySet);
	}
    }

    /**
     * Make a copy of the data (useful for debugging).
     *
     * @return Copy of the value
     */
    @Override public PaxosValue copy() {
	CommutativeSet newSet = 
	    new CommutativeSet();

	newSet.setSlot(slot);
	for(Proposal p : set.keySet()) {
	    Proposal cp = (Proposal)p.copy();

	    for(String id : set.get(p)) {
		newSet.add(cp, new String(id));
	    }
	}
	
	return newSet;
    }

    /**
     * Compare the commutative sets. Instead of doing an exhaustive
     * equality search on the values, just compare the slots. This makes
     * sense because most likely we are comparing the set against a single proposal.
     * 
     * @param obj Object to compare equality
     * @return Positive if this value is greater, zero if
     * the same, negative otherwise. 
     **/
    @Override public boolean equals(Object obj) {
	PaxosValue pv = (PaxosValue)obj;
	return slot == pv.getSlot();
    }

    /**
     * Get/set the slot. 
     **/
    @Override public void setSlot(int s) {
	slot = s;
    }
    @Override public int getSlot() {
	return slot;
    }
}