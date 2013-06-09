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
import java.util.Collection;
import java.nio.ByteBuffer;

/**
 * A simple value container that encapsulates primitive values for state machines.
 */
public class PrimitivePaxosValue implements PaxosValue {
    /**
     * Keep track of the different primitive types. 
     */
    public static final int BOOLEAN    = 0;
    public static final int INT        = 1;
    public static final int DOUBLE     = 2;
    public static final int BYTEARRAY  = 3;
    public static final int SERIALIZED = 4;

    private boolean bv;
    private int iv;
    private double dv;
    private byte[] av;
    private int type;

    /**
     * @param value "int" value
     */
    public PrimitivePaxosValue(int value) {
	type = INT;
	iv = value;

	bv = true;
	dv = 0.00;
	av = null;
    }

    /**
     * @param value "double" value
     */
    public PrimitivePaxosValue(double value) {
	type = DOUBLE;
	dv = value;

	iv = 0;
	bv = true;
	av = null;
    }

    /**
     * @param value "boolean" value
     */
    public PrimitivePaxosValue(boolean value) {
	type = BOOLEAN;
	bv = value;

	dv = 0;
	iv = 0;
	av = null;
    }

    /**
     * @param value "byte array" value
     */
    public PrimitivePaxosValue(byte[] value) {
	type = BYTEARRAY;
	av = value;

	bv = true;
	dv = 0;
	iv = 0;
    }

    /**
     * Set the value.
     */
    public void setIntValue(int v) {
	iv = v;
    }
    public void setDoubleValue(double v) {
	dv = v;
    }
    public void setBoolValue(boolean v) {
	bv = v;
    }
    public void setArrayValue(byte[] v) {
	av = v;
    }

    /**
     * Get the value.
     */
    public int intValue() {
	return iv;
    }
    public double doubleValue() {
	return dv;
    }
    public boolean boolValue() {
	return bv;
    }
    public byte[] arrayValue() {
	return av;
    }

    /**
     * Get the type of this primitive value.
     */
    public void setType(int type) {
	this.type = type;
    }
    public int getType() {
	return type;
    }

    /**
     * Normally returns the raw value. However, this method should
     * not be used with the primitive type. 
     */
    @Override public byte[] getValue() {
	return null;
    }

    /**
     * Get the collection of values.
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
     * Serialize the value.
     *
     * @return Serialized value
     */
    @Override public byte[] serialize() {
	ByteBuffer buffer = null;

	if(type == INT) {
	    buffer = ByteBuffer.allocate(2 * (Integer.SIZE / 8));
	    buffer.putInt(type);
	    buffer.putInt(iv);
	}
	else if(type == DOUBLE) {
	    buffer = ByteBuffer.allocate((Integer.SIZE / 8) + 
					 (Double.SIZE / 8));
	    buffer.putInt(type);
	    buffer.putDouble(dv);
	}
	else if(type == BOOLEAN) {
	    buffer = ByteBuffer.allocate((Integer.SIZE / 8) + 
					 (Character.SIZE / 8));
	    buffer.putInt(type);
	    if(bv) {
		buffer.putChar((char)1);
	    }
	    else {
		buffer.putChar((char)0);
	    }
	}
	else if(type == BYTEARRAY) {
	    buffer = ByteBuffer.allocate((Integer.SIZE / 8) + 
					 (Integer.SIZE / 8) + 
					 av.length);
	    buffer.putInt(type);
	    buffer.putInt(av.length);
	    buffer.put(av);
	}

	if(buffer != null) {
	    return buffer.array();
	}
	else {
	    return null;
	}
    }

    /**
     * Do not do implement for primitive values. Use deSerializePrimitive
     * instead. 
     */
    public void unSerialize(byte[] data) {
    }

    /**
     * Make a copy of the data.
     *
     * @return Copy of the value
     **/
    @Override public PaxosValue copy() {
	PrimitivePaxosValue v = null;

	if(type == INT) {
	    v = new PrimitivePaxosValue(iv);
	}
	else if(type == DOUBLE) {
	    v = new PrimitivePaxosValue(dv);
	}
	else if(type == BOOLEAN) {
	    v = new PrimitivePaxosValue(bv);
	}
	else if(type == BYTEARRAY) {
	    v = new PrimitivePaxosValue(Arrays.copyOf(av, av.length));
	}

	return v;
    }

    /**
     * Get/set the slot. 
     **/
    @Override public void setSlot(int s) {
    }
    @Override public int getSlot() {
	return -1;
    }

    /**
     * Change the paxos value back into the proper primitive type.
     *
     * @param Serialized primitive value
     * @return Unserialized primitive value
     */
    public static PrimitivePaxosValue deSerializePrimitive(PrimitivePaxosValue value) {
	if(value.getType() != PrimitivePaxosValue.SERIALIZED) {
	    // There is nothing to de-serialize. 
	    return value;
	}

	ByteBuffer buffer = ByteBuffer.wrap(value.arrayValue());
	int type = buffer.getInt();

	if(type == INT) {
	    return new PrimitivePaxosValue(buffer.getInt());
	}
	else if(type == DOUBLE) {
	    return new PrimitivePaxosValue(buffer.getDouble());
	}
	else if(type == BOOLEAN) {
	    char c = buffer.getChar();
	    return new PrimitivePaxosValue(c != 0);
	}
	else if(type == BYTEARRAY) {
	    int length = buffer.getInt();
	    byte[] av = new byte[length];
	    buffer.get(av);

	    return new PrimitivePaxosValue(av);
	}

	return null;
    }

    /**
     * Change the paxos value back into a ballot.
     *
     * @param value Serialized ballot value.
     */
    public static Ballot deSerializeBallot(PaxosValue value) {
	Ballot ballot = null;
	if(value instanceof Ballot) {
	    ballot = (Ballot)value;
	}
	else {
	    // First de-serialize the message payload
	    // back into a ballot. 
	    PrimitivePaxosValue prim = (PrimitivePaxosValue)value;
	    byte[] data = prim.arrayValue();
	    ballot = new Ballot();
	    ballot.unSerialize(data);
	}

	return ballot;
    }

    /**
     * Change the paxos value back into a proposal. 
     *
     * @param value Serialized proposal value.
     */
    public static Proposal deSerializeProposal(PaxosValue value) {
	Proposal proposal = null;
	if(value instanceof Proposal) {
	    proposal = (Proposal)value;
	}
	else {
	    // First de-serialize the message payload.
	    PrimitivePaxosValue prim = (PrimitivePaxosValue)value;
	    byte[] data = prim.arrayValue();
	    proposal = new Proposal();
	    proposal.unSerialize(data);
	}

	return proposal;
    }    

    /**
     * Change the paxos value back into a commutative set. 
     *
     * @param value Serialized commutative set.
     */
    public static CommutativeSet deSerializeSet(PaxosValue value) {
	CommutativeSet set = null;
	if(value instanceof CommutativeSet) {
	   set = (CommutativeSet)value;
	}
	else {
	    // First de-serialize the message payload.
	    PrimitivePaxosValue prim = (PrimitivePaxosValue)value;
	    byte[] data = prim.arrayValue();

	    set = new CommutativeSet();
	    set.unSerialize(data);
	}

	return set;
    }
}