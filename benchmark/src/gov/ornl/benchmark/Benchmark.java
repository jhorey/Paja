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

package gov.ornl.paja.benchmark;

/**
 * Java libs.
 */
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Paxos libs.
 */
import gov.ornl.paja.proto.PaxosStateMachine;
import gov.ornl.paja.proto.PaxosStateMachineFactory;
import gov.ornl.paja.proto.CommutativeOperator;
import gov.ornl.paja.proto.CommutativeSet;
import gov.ornl.paja.proto.Proposal;
import gov.ornl.paja.proto.PaxosValue;
import gov.ornl.paja.roles.Client;
import gov.ornl.paja.node.PaxosNode;
import gov.ornl.paja.node.PaxosGroup;
import gov.ornl.paja.node.PaxosDiagnoser;

/**
 * Starts servers and clients, and runs a few benchmarks. Also
 * does some validation testing to make sure all the replicas 
 * have the same state. 
 *
 * @author James Horey
 */
public class Benchmark {
    /**
     * General debug logger. 
     */
    private static Logger logger = 
	Logger.getLogger("Benchmark"); 

    private boolean fastMode;
    private String configDir;
    private List<BenchmarkServer> servers;
    private List<BenchmarkClient> clients;
    private BenchmarkDiagnoser diagnoser;

    /**
     * @param configDir Configuration file for the Paxos nodes. 
     */
    public Benchmark(String configDir) {
	this.configDir = configDir;

	servers = new ArrayList<>();
	clients = new ArrayList<>();
    }

    /**
     * Start some servers. 
     */
    public void startServers(boolean fastMode) {
	try {
	    DirectoryStream<Path> stream = 
		Files.newDirectoryStream(Paths.get(configDir).toAbsolutePath());

	    int i = 0;
	    for(Path entry : stream) {
		BenchmarkServer server = 
		    new BenchmarkServer(i++, entry, fastMode);
		servers.add(server);
	    }
	} catch(Exception e) {
	    e.printStackTrace();
	}

	// Start the servers.
	for(BenchmarkServer s : servers) {
	    new Thread(s).start();
	}

	// Wait a second for the servers to start. 
	try { Thread.sleep(256); }
	catch(Exception e) { }

	// Elect the leader. We do this after starting the
	// servers so that all the communication is set up properly. 
	for(BenchmarkServer s : servers) {
	    s.electLeader();
	}

	// Start the fast window.
	for(BenchmarkServer s : servers) {
	    s.updateFastWindow();
	}
    }

    /**
     * Start some clients.
     */
    public void startClients(int num, boolean fastMode) {
	this.fastMode = fastMode;

	for(int i = 0; i < num; ++i) {
	    BenchmarkClient node = 
		new BenchmarkClient(i); 
	    clients.add(node);
	}

	// Start the benchmark. 
	for(BenchmarkClient c : clients) {
	    new Thread(c).start();
	}
    }

    /**
     * Diagnose the network. 
     */
    public void startDiagnosis(String cmd) {
	diagnoser = new BenchmarkDiagnoser();

	if(cmd.equals("print")) {
	    diagnoser.printState();
	}
	else if(cmd.equals("stop")) {
	    diagnoser.stopMajority(false);
	}
	else if(cmd.equals("stopnode")) {
	    diagnoser.stopMinority(true);
	}
	else if(cmd.equals("recover")) {
	    diagnoser.recover();
	}
    }

    /**
     * Help control and benchmark the network. 
     */
    class BenchmarkDiagnoser {
	private PaxosDiagnoser diagnoser;

	public BenchmarkDiagnoser() {
	    diagnoser = new PaxosDiagnoser(configDir);
	    diagnoser.getNetwork().electLeader();
	}

	/**
	 * Generate some benchmark data and submit it to 
	 * the Paxos group. 
	 */
	public void printState() {
	    // Ask the replicas to print out their values. We do
	    // this to make sure that all replicas have all the values. 
	    diagnoser.printState();
	}

	/**
	 * Stop majority of the nodes. 
	 */
	public void stopMajority(boolean leader) {
	    diagnoser.stopMajority(leader);
	}

	/**
	 * Stop minority of the nodes. 
	 */
	public void stopMinority(boolean leader) {
	    diagnoser.stopMinority(leader);
	}

	/**
	 * Restart crashed nodes. 
	 */
	public void recover() {
	    diagnoser.recover();
	}
    }

    /**
     * An example client implementation. Our client just generates
     * a bunch of sequential values.
     */
    class BenchmarkClient implements Runnable {
	private static final int MAX_SUBMISSIONS = 10;
	private PaxosGroup group;
	private String id;

	public BenchmarkClient(int id) {
	    this.id = new String("c" + id);
	    group = new PaxosGroup(configDir);
	    group.setClientID(this.id);
	    group.setDiagnosisMode(true);
	    group.getNetwork().electLeader();
	}

	/**
	 * Generate some fake data. 
	 */	
	private byte[] generateData(int index) {
	    byte[] idBuf = null;
	    if(!fastMode) {
		idBuf = id.getBytes();
	    }
	    else {
		idBuf = "0".getBytes();
	    }

	    ByteBuffer buffer = ByteBuffer.allocate(idBuf.length + 
						    (Integer.SIZE / 8));
	    
	    buffer.put(idBuf);
	    buffer.putInt(index);

	    return buffer.array();
	}

	/**
	 * Generate some benchmark data and submit it to 
	 * the Paxos group. 
	 */
	public void run() {
	    group.connect();

	    // Take some time for the first submission so that it is 
	    // fast submitted, we have time to receive the fast window. 
	    byte[] data = generateData(0);
	    group.propose(data);

	    // try { Thread.sleep(256); }
	    // catch(InterruptedException e) { }

	    // Submit the rest of the proposals. 
	    for(int i = 1; i < MAX_SUBMISSIONS; ++i) {
		data = generateData(i);
		group.propose(data);
	    }
	}
    }

    /**
     * Example benchmark server that starts a single Paxos node. 
     */
    class BenchmarkServer implements Runnable {
	private static final int FAST_WINDOW_SIZE = 20;

	private boolean fastMode;
	private PaxosNode node;
	private Path entry;
	private int id;

	public BenchmarkServer(int id, Path entry, boolean fastMode) {
	    this.id = id;
	    this.entry = entry;
	    this.fastMode = fastMode;
	    node = new PaxosNode(entry.toString()); 
	    node.setDiagnosisMode(true);
	    node.setStateMachineFactory(new ExampleStateMachineFactory());
	}

	/**
	 * Get Paxos node ID of this server. 
	 */
	public String getID() {
	    return node.getID();
	}

	/**
	 * Set the leader ID. 
	 */
	public void electLeader() {
	    node.getNetwork().electLeader();
	    node.setLeader(node.getNetwork().getLeader());
	}

	/**
	 * Create the Paxos node. The Paxos node automatically
	 * starts listening for messages. 
	 */
	public void run() {
	    // Start the server. 
	    node.start();	    
	}

	/**
	 * update the fast window. 
	 */
	public void updateFastWindow() {
	    if(fastMode) {
		// We are running this server in fast mode. 
		// That means we should set the window size. 
		node.updateFastWindow(FAST_WINDOW_SIZE);
	    }
	}
    }

    /**
     * Create state machines for the replicas.
     */
    class ExampleStateMachineFactory implements PaxosStateMachineFactory {
	/**
	 * Create a new state machine. 
	 */
	public PaxosStateMachine newStateMachine() {
	    return new ExampleStateMachine();
	}
    }

    /**
     * Example state machine. It just keeps track of all the values
     * in a long list. 
     */ 
    class ExampleStateMachine implements PaxosStateMachine {
	private ConcurrentSkipListMap<Integer, byte[]> states;
	private volatile int numApplied;
	
	public ExampleStateMachine() {
	    states = new ConcurrentSkipListMap<>();
	    numApplied = 0;
	}

	/**
	 * Apply the supplied value to the state machine. 
	 **/
	@Override public void apply(byte[] value) {
	    states.put(numApplied++, value);
	    logger.log(Level.INFO, String.format("applied (%d %s)", 
						 numApplied, Arrays.toString(value)));
	}

	/**
	 * Get the output associated with the most recently transition.
	 **/
	@Override public Object getValue() {
	    return null;
	}

	/**
	 * Print out the current state of the state machine. 
	 */
	@Override public void print() {
	    System.out.printf("--------------------------------------\n");
	    System.out.printf("numApplied:%d\n", numApplied);
	    System.out.printf("--------------------------------------\n");
	}
    }

    /**
     * Start the benchmark either in "client" mode or
     * "server" mode. Client mode starts a specified number of
     * clients that attempt to make proposals. Server mode starts
     * a few Paxos nodes and elects one the leader. 
     */
    public static void main(String[] args) {
	String mode = args[0].trim();
	String config = args[1].trim();

	Benchmark benchmark = new Benchmark(config);
	if(mode.equals("client")) {
	    int num = Integer.parseInt(args[2].trim());
	    benchmark.startClients(num, false);
	}
	else if(mode.equals("fastclient")) {
	    int num = Integer.parseInt(args[2].trim());
	    benchmark.startClients(num, true);
	}
	else if(mode.equals("server")) {
	    benchmark.startServers(false);
	}
	else if(mode.equals("fastserver")) {
	    benchmark.startServers(true);
	}
	else if(mode.equals("diagnose")) {
	    benchmark.startDiagnosis(args[2].trim());
	}
    }
}