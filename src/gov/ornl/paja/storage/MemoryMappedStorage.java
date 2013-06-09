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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Memory-mapped implementation of the Paxos storage interface. 
 */
public class MemoryMappedStorage implements PaxosStorage {
    /**
     * Maximum memory mapped buffer size. Set to a reasonable
     * size so that we don't continually re-map, but not too large. 
     *
     * Currently about 4MB. In the future, probably should be a parameter.
     */
    private static long MAX_MAP_SIZE = 4096;

    private volatile int logID; // Current log ID
    private String logPath; // Path to the log file. 

    private FileChannel logChannel; // Open channel to current log. 
    private MappedByteBuffer logBuffer; // Buffer to log channel. 
    private long mapStart; // Where we are in the WAL. 
    private long appendStart;

    public MemoryMappedStorage() {
	logID = 0;
	logPath = null;
	logBuffer = null;
	logChannel = null;
	mapStart = 0;
	appendStart = 0;
    }

    /**
     * Get/set the log file name. 
     * 
     * @param file Name of the log file
     */
    public void setLogFile(String file) {
	logPath = file;
    }
    public String getLogFile() {
	return logPath;
    }

    /**
     * Set the start of the append operation. 
     *
     * @param start Starting position
     */
    public void setAppendStart(long start) {
	appendStart = start;
    }

    /**
     * Set the latest log ID. Used during recovery.
     *
     * @param logID Initial log message ID to use
     */
    public void setLogID(int logID) {
	this.logID = logID;
    }

    /**
     * Start a new log file. 
     */
    public void startNewLog() {
	open();
    }

    /**
     * Re-use the existing log file, and just
     * append values. 
     */
    public void reuseLog() {
	mapStart = appendStart;
	open();
    }

    /**
     * Open the log file. 
     */
    private void open() {
	Path path = Paths.get(logPath).toAbsolutePath();

	// Open flags. "d" is for durable. 
	String flag = "rw";
	// flag += "d";

	try {
	    // Check if the log already exists. If not,
	    // go ahead and create one. 
	    if(!Files.exists(path)) {
		Files.createFile(path);
	    }

	    // Open up the log file. 
	    logChannel = new RandomAccessFile(path.toString(), flag).getChannel();

	    // Map the file. 
	    logBuffer = mapWAL();
	} catch(IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Remap the WAL buffer. 
     */
    private MappedByteBuffer mapWAL() {
	MappedByteBuffer buf = null;

	try {
	    if(logBuffer != null) {
		logBuffer.force();
	    }

	    buf = 
		logChannel.map(FileChannel.MapMode.READ_WRITE, mapStart, MAX_MAP_SIZE);
	} catch(IOException e) {
	    e.printStackTrace();
	}

	if(buf != null) {
	    mapStart += MAX_MAP_SIZE;
	}

	return buf;
    }

    /**
     * Open the log file using a normal filechannel.
     */
    private FileChannel openLog() {
	Path path = Paths.get(logPath).toAbsolutePath();

	try {
	    return FileChannel.open(path, 
				    StandardOpenOption.READ);
	} catch(IOException e) {
	    e.printStackTrace();
	}

	return null;
    }

    /**
     * Shut down the storage. 
     */
    public void close() {
	try {
	    logBuffer.force();
	    logChannel.close();
	} catch(IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Play back the log. 
     *
     * @return Iterator over log messages
     */
    public PaxosStorageIterator playback() {
	return new PaxosStorageIterator(openLog());
    }

    /**
     * Log the following message with the supplied ID. Return
     * the log number of the message. 
     *
     * @param id Application-specific ID associated with this log
     * @param msg Message to log
     * @return The log message ID used to store this message
     */
    public int log(byte[] id, byte[] msg) {
	if(logPath == null) {
	    return -1;
	}

	synchronized(this) { 
	    int size = 
		(Short.SIZE / 8) + 
		3 * (Integer.SIZE / 8) +
		id.length + 
		msg.length;

	    ByteBuffer buf = ByteBuffer.allocateDirect(size);
	    buf.putShort(MAGIC_NUMBER); // Magic number.
	    buf.putInt(logID);
	    buf.putInt(id.length);
	    buf.putInt(msg.length);	
	    buf.put(id);
	    buf.put(msg);

	    if(logBuffer.remaining() < buf.position()) {
		logBuffer = mapWAL();
	    }

	    buf.flip();
	    logBuffer.put(buf);
	}

	// Increment the Log ID. 
	return logID++;
    }
}
