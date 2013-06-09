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

package gov.ornl.paja.storage;

/**
 * Java libs.
 **/
import java.util.Iterator;
import java.util.Arrays;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel;

/**
 * Iterate over the log messages in log ID order. 
 */
public class PaxosStorageIterator {
    private FileChannel chan;
    private ByteBuffer current;
    private LogMessage nextLogMessage;
    private long bytesRead;
    
    /**
     * @param channel File channel where the data is stored
     */
    public PaxosStorageIterator(FileChannel channel) {
	chan = channel;
	current = ByteBuffer.allocate( (Short.SIZE / 8) +
				       (3 * Integer.SIZE) / 8 );
	nextLogMessage = null;
	bytesRead = 0;
    }

    /**
     * Is there another log message?
     **/
    public boolean hasNext() {
	// Reset the buffer position.
	current.clear();
	int s = 0;

	// Try to read in another buffer.
	try {
	    s = chan.read(current);
	} catch(IOException e) {
	    e.printStackTrace();
	}

	// Go back to the beginning and read the 
	// magic value.
	current.flip();

	if(s == 0) {
	    return false;
	}
	else {
	    short magic = current.getShort();
	    return magic == PaxosStorage.MAGIC_NUMBER;
	}
    }

    /**
     * Get the number of bytes read so far. 
     */
    public long getReadBytes() {
	return bytesRead;	
    }

    /**
     * Get the next log message.
     **/
    private LogMessage recoverMessage() throws PaxosStorageException {
	int logID = 0;
	int idSize = 0;
	int msgSize = 0;

	logID = current.getInt();
	idSize = current.getInt();
	msgSize = current.getInt();
	bytesRead += current.capacity();

	// Allocate the right sized buffers.
	ByteBuffer idBuf = ByteBuffer.allocate(idSize);
	ByteBuffer msgBuf = ByteBuffer.allocate(msgSize);

	// Read the rest of the log message.
	try {
	    bytesRead += chan.read(idBuf);
	    bytesRead += chan.read(msgBuf);
	} catch(IOException e) {
	    e.printStackTrace();
	}

	// Reset the buffers. 
	idBuf.flip();
	msgBuf.flip();

	// Get the ID and message arrays.
	byte[] id = idBuf.array();
	byte[] msg = msgBuf.array();

	return new LogMessage(logID, id, msg);

    }

    /**
     * Get the next log message.
     **/
    public LogMessage next() throws PaxosStorageException {
	return recoverMessage();
    }

    /**
     * Remove an element.
     **/
    public void remove() {
	// Do not implement
    }
}