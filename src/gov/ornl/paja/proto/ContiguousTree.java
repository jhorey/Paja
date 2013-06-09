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

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

/**
 * A contiguous tree stores a set of values in sorted order. To save
 * space it collapses contiguous values and represents them as a range. 
 * It also associates a set of authors for each value. 
 * 
 * @author James Horey
 */
public class ContiguousTree  {
    /**
     * Pointer to parent node. Null if root. 
     */ 
    private ContiguousTree parent;

    /**
     * Left and right child nodes. 
     */
    private ContiguousTree left;
    private ContiguousTree right;

    /**
     * Min and max values (inclusive). 
     */
    private int minValue;
    private int maxValue;

    /**
     * Indicate whether this tree is initialized. 
     */
    private boolean init;

    /**
     * Authors of these values. 
     */
    private Set<String> authors;

    public ContiguousTree() {
	parent = null;
	left = null;
	right = null;
	
	minValue = 0;
	maxValue = 0;
	authors = new HashSet<>();
	init = false;
    }

    /**
     * Get the range of values stored in this node. 
     */
    public int getMaxValue() {
	return maxValue;
    }
    public int getMinValue() {
	return minValue;
    }

    /**
     * Get/set the child nodes. 
     */
    public ContiguousTree getLeft() {
	return left;
    }
    public void setLeft(ContiguousTree child) {
	left = child;
    }
    public ContiguousTree getRight() {
	return right;
    }
    public void setRight(ContiguousTree child) {
	right = child;
    }

    /**
     * Get/set the parent node. The parent is null
     * if the tree is a root. 
     */
    public ContiguousTree getParent() {
	return parent;
    }
    public void setParent(ContiguousTree parent) {
	this.parent = parent;
    }

    /**
     * Get the leftmost/right most node in the tree. 
     */
    private ContiguousTree getLeftmost() {
	if(left == null) {
	    return this;
	}
	else {
	    return left.getLeftmost();
	}
    }
    private ContiguousTree getRightmost() {
	if(right == null) {
	    return this;
	}
	else {
	    return right.getRightmost();
	}
    }

    /**
     * Add a value to the tree. May trigger some
     * collapses. 
     */
    public void add(int value, Collection<String> author) {
	// Record who has added the value. 
	if(author != null) {
	    authors.addAll(author);
	}

	if(!init) {
	    init = true;
	    minValue = maxValue = value;
	}
	else {
	    if(minValue <= value &&
	       value <= maxValue) {
		// This value is already in the range. 
		// Don't do anything else. 
		return;
	    }
	    else if(value == maxValue + 1 ||
		    value == minValue - 1) {
		// Extend the current range. 
		init = true;
		extendRange(value);
	    }
	    else {
		init = true;

		// This value is not contiguous. If the value
		// is less than the current range, use the 
		// left child. Otherwise use the right child. 
		if(value < minValue) {
		    if(left == null) {
			left = new ContiguousTree();
			left.setParent(this);
		    }

		    left.add(value, author);
		}
		else {
		    if(right == null) {
			right = new ContiguousTree();
			right.setParent(this);
		    }

		    right.add(value, author);
		}
	    }
	}

    }

    /**
     * Helper method to extend the current range. After extending
     * the range, it also checks it needs to merge any of the child nodes. 
     */
    private void extendRange(int value) {
	if(value == maxValue + 1) {
	    maxValue++;
	}
	else {
	    minValue--;
	}

	// First merge the left child. Get the maximum node 
	// from the left child and see if we can merge the node.
	// If we can, the merge logic looks like this:
	// (1) Merge with current range
	// (2) Node.Parent.Right <= Node.Left
	if(left != null) {
	    ContiguousTree node = left.getRightmost();

	    if(tryMerge(node)) {
		// Check if this node is the current left child.
		// If so, set the left child of the root node.
		if(node == left) {
		    this.setLeft(node.getLeft());
		}
		else {
		    node.getParent().setRight(node.getLeft());
		}
	    }
	}

	// Now merge the right child. The logic is a mirror image
	// of the left child merge. 
	if(right != null) {
	    ContiguousTree node = right.getLeftmost();

	    if(tryMerge(node)) {
		if(node == right) {
		    this.setRight(node.getRight());
		}
		else {
		    node.getParent().setLeft(node.getRight());
		}
	    }
	}
    }

    /**
     * Indicates whether the value is contained this tree. 
     */
    public boolean contains(int value) {
	if(minValue <= value &&
	   value <= maxValue) {
	    return true;
	}
	else if(left != null &&
		value < minValue) {
	    return left.contains(value);
	}
	else if(right != null &&
		value > maxValue) {
	    return right.contains(value);
	}
	else {
	    return false;
	}
    }

    /**
     * Indicate whether the supplied role has authored
     * a value in the tree. This is a recursive implementation!
     */
    public boolean hasAuthored(String author) {
	if(authors.contains(author)) {
	    return true;
	}

	if(left != null) {
	    if(left.hasAuthored(author)) {
		return true;
	    }
	}
	if(right != null) {
	    if(right.hasAuthored(author)) {
		return true;
	    }
	}

	return false;
    }

    /**
     * Get the set of authors. Authors keep track of
     * who submitted values.
     *
     * @return Set of authors
     */
    public Set<String> getAuthors() {
	return authors;
    }

    /**
     * Get the least contiguous range. 
     */
    public int getLeastContiguous() {
	// First get the left-most node. 
	ContiguousTree leftMost = getLeftmost();

	// Get the greatest value in the left-most node.
	return leftMost.getMaxValue();
    }

    /**
     * Determine ii the tree can be merged, and merge the
     * trees if they can be merged. 
     */
    private boolean tryMerge(ContiguousTree otherTree) {

	if(otherTree.getMaxValue() >= minValue - 1) {
	    // Make sure the other tree's min value is 
	    // also in range. 
	    if(otherTree.getMinValue() <= maxValue + 1) {
		minValue = otherTree.getMinValue() < minValue?
		    otherTree.getMinValue() : minValue;
		maxValue = otherTree.getMaxValue() > maxValue?
		    otherTree.getMaxValue() : maxValue;

		return true;
	    }
	}

	return false;
    }

    /**
     * Print contents of the tree. 
     */
    public void print() {
	if(parent == null) {
	    System.out.printf("[");
	}

	if(left != null) {
	    left.print();
	}

	if(minValue != maxValue) {
	    System.out.printf(" %d <-> %d ", minValue, maxValue);
	}
	else {
	    System.out.printf(" %d ", minValue);
	}

	if(right != null) {
	    right.print();
	}

	if(parent == null) {
	    System.out.printf("]\n");
	}
    }

    /**
     * Test contiguous tree. 
     */
    public static void main(String[] args) {
	List<String> role = new ArrayList<>(1);
	role.add("0");
	ContiguousTree tree = null;

	// First test single contiguous set.
	tree = new ContiguousTree();
	for(int i = 0; i < 100; ++i) {
	    tree.add(i, role);
	}
	tree.print();

	// Now test completely non-contiguous set. 
	tree = new ContiguousTree();
	for(int i = 0; i < 8; i += 2) {
	    tree.add(i, role);
	}
	for(int i = -8; i < 0; i += 2) {
	    tree.add(i, role);
	}
	tree.print();

	// Now test hybrid. 
	tree = new ContiguousTree();
	for(int i = 0; i < 4; ++i) {
	    tree.add(i, role);
	}
	for(int i = -6; i < -3; ++i) {
	    tree.add(i, role);
	}
	for(int i = 6; i < 10; ++i) {
	    tree.add(i, role);
	}
	tree.print();

	// Now test merge root.
	tree.add(4, role);
	tree.add(5, role);
	tree.print();
    }

}