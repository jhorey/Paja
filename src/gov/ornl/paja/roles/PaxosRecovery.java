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
import gov.ornl.paja.storage.PaxosStorage;
import gov.ornl.paja.storage.MemoryMappedStorage;
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.proto.Proposal;
import gov.ornl.paja.proto.Ballot;

/**
 * Java libs.
 **/
import java.nio.ByteBuffer;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.BasicFileAttributeView;

/**
 * Abstract class to handle logging and recovering for all
 * different Paxos roles. 
 *
 * @author James Horey
 */
public abstract class PaxosRecovery {
    /**
     * Durable storage for recovery. 
     */
    protected PaxosStorage storage;

    /**
     * Log all the errors, warnings, and messages.
     */
    protected static Logger logger = 
	Logger.getLogger("PaxosRole.Recovery"); 

    /**
     * Default constructor.
     * Currently uses the "memory mapped" storage implementation.
     * In the future, we should consider making this more modular. 
     */
    public PaxosRecovery() {
	storage = new MemoryMappedStorage();
    }

    /**
     * Get the last modified file.
     *
     * @param The directory to find the log file
     * @return Path to the latest file
     */
    public Path getLatestFile(Path dir) {
	long greatTime = 0;
	Path greatPath = null;

	try {
	    DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "log*");
	    for(Path entry: stream) {
		BasicFileAttributes v
		    = Files.getFileAttributeView(entry, BasicFileAttributeView.class)
		    .readAttributes();
		
		if(v.lastModifiedTime().toMillis() > greatTime) {
		    greatTime = v.lastModifiedTime().toMillis();
		    greatPath = entry;
		}
	    }
	}
	catch(Exception e) {
	    e.printStackTrace();
	}

	return greatPath;
    }

    /**
     * Set the log file. 
     * 
     * @param file Name of the log file. 
     */
    public void setLogFile(String file) {
	// Check if the log file is a directory or not. 
	// If it is a directory, then choose the latest file. 
	Path path = Paths.get(file).toAbsolutePath();
	if(Files.isDirectory(path)) {
	    Path logFile = getLatestFile(path);
	    storage.setLogFile(logFile.toString());
	}
	else {
	    storage.setLogFile(file);
	}
    }

    /**
     * Start a new log.
     */
    public void startNewLog() {
	storage.startNewLog();
    }

    /**
     * Re-use an existing log.
     */
    public void reuseLog() {
	storage.reuseLog();
    }

    /**
     * Log the ballot to storage. Will log this using the
     * default "ballot" key. 
     *
     * @param ballot The ballot to log. 
     */
    public void log(Ballot ballot) {
	storage.log("ballot".getBytes(), 
		    ballot.serialize());
    }

    /**
     * Log the value with a specific label. 
     *
     * @param label Label to use
     * @param value Value to log
     */
    public void log(String label, PaxosValue value) {
	storage.log(label.getBytes(),
		    value.serialize());
    }

    /**
     * Log the value with the default label. The default
     * label changes depending on the value type. 
     *
     * @param value Value to log
     */
    public void log(PaxosValue value) {
	// Is this is a proposal or a commutative set? 
	String key = null;
	if(value instanceof Proposal) {
	    key = "proposal";
	}
	else {
	    key = "set";
	}

	log(key, value);
    }

    /**
     * Log the slot value. Uses the default label of "slot". 
     *
     * @param slot Slot value
     */
    public void log(int slot) {
	ByteBuffer d = ByteBuffer.allocate(4).putInt(slot);
	storage.log("slot".getBytes(), d.array()); 
    }

    /**
     * Log a message. Uses the default label of "msg". 
     *
     * @param msg The message to log
     */
    public void log(PaxosMessage msg) {
	storage.log("msg".getBytes(),
		    msg.serialize());
    }

    /**
     * Perform the recovery from log
     */
    public abstract void recover();
}