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
 */
import java.util.Iterator;

/**
 * Stable storage for recovery logs. 
 */
public interface PaxosStorage {
    /**
     * Used to verify that the log message has actual data
     * in it instead of just noise. 
     */
    public static final short MAGIC_NUMBER = 79;

    /**
     * Get/set the log file name. 
     * 
     * @param file Name of the log file
     */
    public void setLogFile(String file);
    public String getLogFile();

    /**
     * Set the latest log ID. Used during recovery.
     *
     * @param logID Initial log message ID to use
     */
    public void setLogID(int logID);

    /**
     * Set the start of the append operation. 
     */
    public void setAppendStart(long start);

    /**
     * Start a new log.
     */
    public void startNewLog();

    /**
     * Re-use the existing log file, and just
     * append values. 
     */
    public void reuseLog();

    /**
     * Play back the log. 
     *
     * @return Iterator over log messages
     */
    public PaxosStorageIterator playback();

    /**
     * Log the following message with the supplied ID. Return
     * the log number of the message. 
     *
     * @param id Application-specific ID associated with this log
     * @param msg Message to log
     * @return The log message ID used to store this message
     */
    public int log(byte[] id, byte[] msg);
}
