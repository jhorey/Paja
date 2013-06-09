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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/**
 * Exception throw when reading/writing to the Paxos log. 
 */
public class PaxosStorageException extends Exception {
    private String cause;

    public PaxosStorageException() {
	super();
	cause = "file unknown";
    }

    /**
     * @param msg Message to deliver to the user
     */
    public PaxosStorageException(String msg) {
	super(msg);
	cause = msg;
    }

    /**
     * Get the error message.
     *
     * @return Message associated with the error
     */
    public String getError() {
	return cause;
    }
}