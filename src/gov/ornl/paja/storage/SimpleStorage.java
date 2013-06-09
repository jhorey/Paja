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
import java.util.SortedMap;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/**
 * A simple logger implementation. Most other implementations will
 * need to integrate with a datastore logger. 
 * 
 * DEPRACATED!!!!
 */
public class SimpleStorage implements PaxosStorage {
    public static final int FLUSH_THRESHOLD = 1;

    private volatile int logID; // Current log ID
    private String logPath; // Path to the log file. 
    private FileChannel chan; // File channel. 
    private SortedMap<Integer, ByteBuffer> log; // Non-flushed log.

    public SimpleStorage() {
	logID = 0;
	logPath = null;
	log = new ConcurrentSkipListMap<Integer, ByteBuffer>();
	chan = null;
    }

    /**
     * Get/set the log file name. 
     **/
    public void setLogFile(String f) {
	logPath = f;
    }
    public String getLogFile() {
	return logPath;
    }

    /**
     * Set the start of the append operation. 
     */
    public void setAppendStart(long start) {
    }

    /**
     * Set the latest log ID. Used during recovery.
     **/
    public void setLogID(int l) {
	logID = l;
    }

    /**
     * Start a new log file. 
     */
    public void startNewLog() {
    }

    /**
     * Re-use the existing log file, and just
     * append values. 
     */
    public void reuseLog() {
    }

    /**
     * Shut down the storage. 
     **/
    public void close() {
	flush(); // First flush everything. 

	try {
	    chan.close(); // Close the channel.
	} catch(IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Play back the log using the optional ID filter. 
     **/
    public PaxosStorageIterator playback() {
	// Flush anything in memory to disk. 
	flush();

	// Create a new iterator with the appropriate file channel. 
	try {
	    FileChannel in = FileChannel.open(Paths.get(logPath), 
					      StandardOpenOption.READ,
					      StandardOpenOption.WRITE);
	    return new PaxosStorageIterator(in);
	} catch(Exception e) {
	    System.out.printf("log: could not open WAL %s\n", logPath);
	    e.printStackTrace();
	}

	return null;
    }

    /**
     * Log the following message with the supplied ID. Return
     * the log number of the message. 
     **/
    public int log(byte[] id, byte[] msg) {
	try {
	    ByteBuffer buf = ByteBuffer.allocate((Short.SIZE / 8) + 
						 (2 * Integer.SIZE) / 8 + 
						 id.length + 
						 msg.length);

	    // Place the log line. 
	    buf.putShort(PaxosStorage.MAGIC_NUMBER);
	    buf.putInt(logID);
	    buf.putInt(id.length);
	    buf.putInt(msg.length);	
	    buf.put(id);
	    buf.put(msg);

	    // Place into the log and flush as necessary. 
	    log.put(logID, buf);
	    if(log.size() > FLUSH_THRESHOLD) {
		flush();
	    }

	    // Increment the Log ID. 
	    return logID++;
	} catch(OutOfMemoryError e) {
	    e.printStackTrace();
	    return -1;
	}
    }

    /**
     * Flush the current log onto disk. 
     **/
    private void flush() {
	if(logPath == null) {
	    return;
	}

	try {
	    if(chan == null) { // Need to open. 
		chan = FileChannel.open(Paths.get(logPath), 
					StandardOpenOption.CREATE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE);
	    }

	    // Write out the buffer contents in log order. 
	    for(Integer logID : log.keySet()) {
		ByteBuffer data = log.get(logID);

		if(data != null) {
		    data.flip();
		    chan.write(data);
		}
	    }
	    log.clear(); // Clear the temporary log. 
	} catch(Exception e) {
	    System.out.printf("could not flush WAL\n");
	    e.printStackTrace();
	}
    }
}
