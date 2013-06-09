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
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * An interface for a fast window policy. The fast window policy is responsible
 * for updating the size of the fast window slots. A large fast window is optimal
 * for clients that have many values to transmit, since they can skip the proposer. 
 * However, large fast windows are prone to conflicts if there are many concurrent
 * clients. Thus, a good fast window policy balances these two needs depending on
 * local conditions. 
 */
public abstract class FastWindowPolicy {
    protected Proposer proposer;

    public FastWindowPolicy(Proposer p) {
	proposer = p;
    }

    /**
     * Main function to update the window information. 
     *
     * @param slot The slot being updated
     * @param numConflicts Number of conflicts for the slot
     */
    public abstract void updateWindow(int slot, int numConflicts);

    /**
     * Inform the window policy of any changes in the available slots. 
     *
     * @param start Start of the new window
     * @param end End of the new window
     */
    public abstract void setFastStart(int start, int end);

    /**
     * Get the current fast window information.
     *
     * @return Two element array. First value is the
     * start and the second value is the end of the window. 
     */
    public abstract int[] getFastWindow();
}