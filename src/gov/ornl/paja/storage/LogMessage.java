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

/**
 * A log message is the thing that gets written to the log. 
 */
public class LogMessage {
    private int logNum; // Current log ID. 
    private byte[] id; // ID of the log message. 
    private byte[] msg; // Actual log message

    /**
     * @param logNum Each log message has a unique log number
     * @param id Application defined identification label
     * @param msg Actual log message
     */
    public LogMessage(int logNum, byte[] id, byte[] msg) {
	this.logNum = logNum;
	this.id = id;
	this.msg = msg;
    }

    /**
     * Get/set the log message ID.
     */
    public void setID(byte[] id) {
	this.id = id;
    }
    public byte[] getID() {
	return id;
    }

    /**
     * Get/set the log message.
     */
    public void setMsg(byte[] msg) {
	this.msg = msg;
    }
    public byte[] getMsg() {
	return msg;
    }

    /**
     * Get/set the log message num.
     */
    public void setNum(int i) {
	logNum = i;
    }
    public int getNum() {
	return logNum;
    }
}