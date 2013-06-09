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

package gov.ornl.paja.network.netty;

/**
 * Java libs.
 */
import java.util.List;
import java.util.Map;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.net.SocketAddress;

/**
 * Netty libs.
 */
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

/**
 * Paxos libs.
 */ 
import gov.ornl.paja.roles.PaxosRole;
import gov.ornl.paja.roles.PaxosCommunicator;
import gov.ornl.paja.roles.PaxosMessage;

/**
 * For logging.
 **/
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A netty-backed implementation of the network communicator. 
 *
 * @author James Horey
 */
public class NettyCommunicator extends PaxosCommunicator {
    private volatile int multicastCount = 0;

    /**
     * General debug logger. 
     */
    private static Logger logger = 
	Logger.getLogger("PaxosCommunicator.Netty"); 

    /**
     * Used for routing. 
     */
    private StaticNetwork network;

    /**
     * The communication channel. 
     */
    private NettyPipeline pipeline;

    /**
     * ID of our "home" node. 
     */
    private String home;

    /**
     * The "upstream" communicator that actually communicates
     * with the Paxos node.
     */
    private PaxosCommunicator upstream;

    /**
     * Factories to create servers & clients. 
     */
    private ChannelFactory clientFactory;
    private ChannelFactory serverFactory;

    /**
     * Handle to the server channel.
     */
    private Channel serverChannel = null;

    /**
     * @param network The network contains routing information. 
     */
    public NettyCommunicator(StaticNetwork network) {
	this.network = network;

	// Initialize the communication pipeline. 
	pipeline = new NettyPipeline();
	pipeline.initServer();
	pipeline.initClient();
    }

    /**
     * Send a message to a non-specific node with the specified role.
     * Usually this means sending to a local role on the same node,
     * unless the sending role is already on the node. 
     */
    public void send(PaxosRole sender, PaxosRole.Role role, PaxosMessage msg) {
	// Ask the network for the "desired" target. Depending on the
	// network implementation, this could be the "leader", etc. 
	StaticNetwork.Route route = network.getDesirable(home);

	if(route != null) {
	    msg.setDestination(role);
	    pipeline.startClient(route.ip, 
				 route.port, 
				 msg);
	}
    }

    /**
     * Send a specific message to the supplied role. 
     * 
     * @param destination Destination role
     * @param msg Message to send
     */
    public void send(PaxosRole destination, PaxosMessage msg) {
	StaticNetwork.Route route = network.getRoute(destination.getID());
	if(route != null) {
	    msg.setDestination(destination.getType());
	    pipeline.startClient(route.ip, 
				 route.port, 
				 msg);
	}
    }

    /**
     * Multi-cast a message to all nodes with a specific role. 
     * 
     * @param role Destination role
     * @param msg Message to send
     * @param policy Quorum policy
     * @return Success code
     */
    public int multicast(PaxosRole.Role role, PaxosMessage msg, Quorum policy) {
	// Get the number of nodes necessary for a quorum. 
	int expected = getQuorum(network.getNumNodes(), policy);
	return multicast(role, msg, expected);
    }

    /**
     * Multi-cast a message to all nodes with a specific role. 
     *
     * @param role Destination role
     * @param msg Message to send
     * @param expected Number of nodes to multicast
     * @return Success code
     */
    public int multicast(PaxosRole.Role role, PaxosMessage msg, int expected) {
	// Get all the nodes in the network. 
	Map<String, StaticNetwork.Route> routes = network.getAllRoutes();

	int i = 0;
	for(String id : routes.keySet()) {

	    if(!id.equals(home)) {
		// Make sure that we are not sending to a client. 
		if(role != PaxosRole.Role.CLIENT &&
		   !network.isClientConnection(id)) {
		    StaticNetwork.Route route = routes.get(id);

		    msg.setDestination(role);
		    pipeline.startClient(route.ip, 
					 route.port, 
					 msg);

		    // Increment the number of messages sent.
		    ++i;
		}
	    }

	    // Only send the minimum necessary.
	    if(i == expected) {
		break;
	    }
	}

	return PaxosCommunicator.SUCCESS;
    }

    /**
     * A message has been successfully received. 
     *
     * @param msg Message that was delivered
     */
    public void updateCallback(PaxosMessage msg) {
	if(upstream != null) {
	    upstream.updateCallback(msg);
	}
    }

    /**
     * A message delivery has failed. 
     * 
     * @param msg Message that failed to deliver
     */
    public void failureCallback(PaxosMessage msg) {
	if(upstream != null) {
	    upstream.failureCallback(msg);
	}
    }

    /**
     * Add an "upstream" communicator. This communicator gets all
     * success/failure messages. 
     *
     * @param com Upstream communicator
     */
    public void addUpstream(PaxosCommunicator com) {
	upstream = com;
    }

    /**
     * Remove an "upstream" communicator.
     *
     * @param com Upstream communicator
     */
    public void removeUpstream(PaxosCommunicator com) {
	upstream = null;
    }

    /**
     * Start listening for network communication. 
     *
     * @param id ID identifying the listening node. 
     */
    public void startListening(String id) {
	home = id;
	StaticNetwork.Route route = network.getRoute(id);
	
	if(route != null) {
	    if(pipeline.startServer(route.port)) {
		logger.log(Level.INFO, String.format("%s: listening port %d", id, route.port));
	    }
	}
    }

    /**
     * Stop listening and close the server channel. 
     */
    public void stopListening() {
	serverChannel.close();
    }

    /**
     * Set up the Netty pipeline to handle communication. 
     **/
    class NettyPipeline {
	private ServerBootstrap server;

	/**
	 * Start a server that handles the supplied protocol. 
	 **/
	public boolean startServer(final int port) {
	    if(server == null) {
		return false; // Did not initialize yet. 
	    }

	    // Create a new connection & state manager. 
	    final CommManager manager = new CommManager();

	    // Create a new pipeline. This time use an anonymous class.
	    server.setPipelineFactory(new ChannelPipelineFactory() {
		    public ChannelPipeline getPipeline() {
			return Channels.pipeline(new PaxosMessageDecoder(),
						 new PaxosMessageEncoder(),
						 manager);
		    }
		});

	    try { // Try to bind to the given port. 
		serverChannel = server.bind(new InetSocketAddress(port)); 
		return true;
	    }
	    catch(Exception e) { 
		e.printStackTrace();
		return false; 
	    }
	}

	/**
	 * Starts the initiation process for synching. 
	 **/
	public void startClient(final String host, 
				final int port, 
				final PaxosMessage msg) {
	    ChannelFuture future;
	    ClientBootstrap client;

	    // Bootstrap a new client. Not sure if the
	    // clientFactory is thread-safe or not, so play it safe.  
	    synchronized(this) {
		client = new ClientBootstrap(clientFactory);
	    }

	    // Set some TCP options. 
	    client.setOption("tcpNoDelay", true);
	    client.setOption("keepAlive", true);

	    // Create a new connection & state manager. 
	    final CommManager manager = new CommManager();
	    client.setPipelineFactory(new ChannelPipelineFactory() {
		    public ChannelPipeline getPipeline() {
			return Channels.pipeline(new PaxosMessageDecoder(),
						 new PaxosMessageEncoder(),
						 manager);
						 
		    }
		});

	    // Include the data we want to send. 
	    manager.start(msg);

	    // Try to connect to the host.
	    future = client.connect(new InetSocketAddress(host, port));
	    future.addListener(new ChannelFutureListener() {
		    public void operationComplete(ChannelFuture f) {
			if(f.isDone() && !f.isSuccess()) {
			    // This is a connection failure. This is not necessarily
			    // the end of the world. Chances are that a client has
			    // shut down and its routing information hasn't been updated yet. 
			    if(!network.updateDisconnected(host, port)) {
			    }
			}
		    }
		}
		);
	}

	/**
	 * Create the bootstrapping, pipeline, etc. 
	 **/
	public void initClient() {
	    clientFactory = 
		new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
						  Executors.newCachedThreadPool());
	}


	/**
	 * The main listening method. Must be called for the node server
	 * to actually start listening for traffic. 
	 **/
	public void initServer() {
	    // Create a new channel factory using cached thread pools for the master & workers.
	    serverFactory = 
		new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
						  Executors.newCachedThreadPool());

	    // Bootstrap a new server. 
	    server = new ServerBootstrap(serverFactory);

	    // Set some TCP options. 
	    server.setOption("child.tcpNoDelay", true);
	    server.setOption("child.keepAlive", true);
	}
    }

    /**
     * Server handler deals with the various state interactions, so it
     * needs a handle to the node server. 
     */
    class CommManager extends SimpleChannelHandler implements ChannelFutureListener {
	private PaxosMessage msg; // Message we are trying to send. 

	public CommManager() {
	    super();
	    msg = null;
	}

	/**
	 * Start the state machine with this message.
	 **/
	public void start(PaxosMessage msg) {
	    this.msg = msg;
	}

	/**
	 * Indicates when the channel is available for writing. 
	 **/    
	@Override public void channelConnected(ChannelHandlerContext ctx,
					       ChannelStateEvent e) {
	    if(msg != null) {
		write(ctx.getChannel(), msg);
	    }
	}

	/**
	 * Indicates when the channel is available for writing. 
	 **/    
	@Override public void channelDisconnected(ChannelHandlerContext ctx,
						  ChannelStateEvent e) {
	    // Channel has been disconnected.
	}

	/**
	 * The write operation has completed. 
	 */
	@Override public void operationComplete(ChannelFuture future) throws Exception {
	    // Try closing the channel so that we don't leak descriptors. 
	    future.getChannel().close();
	}

	/**
	 * Send the message over the channel specified by the message. 
	 **/
	protected void write(Channel channel, PaxosMessage msg) {
	    ChannelFuture future;

	    // Send out a command. 
	    future = channel.write(msg); // Write the contents to the channel. 
	    future.addListener(this);
	}

	/**
	 * The server has received a message.
	 */    
	@Override public void messageReceived(ChannelHandlerContext ctx,
					      MessageEvent e) {
	    PaxosMessage msg;

	    // Get our consensus message and hand it back to the parent.
	    msg = (PaxosMessage)e.getMessage();
	    if(msg != null) {
		// Try to get remote connection information.
		// This is used if we need to update the network
		// information later.
		Channel channel = ctx.getChannel();
		InetSocketAddress remote = 
		    (InetSocketAddress)channel.getRemoteAddress();

		// Update the message with the remote address.
		msg.setRemoteAddress(remote.getAddress().getHostAddress());

		// Update the client. 
		updateCallback(msg);

		// Close the channel so that we don't leak descriptors. 
		channel.close();
	    }
	}

	/**
	 * Something has happened to one of the open channels. Just shut it down. 
	 **/    
	@Override public void exceptionCaught(ChannelHandlerContext ctx,
					      ExceptionEvent e) {
	    e.getChannel().close(); 
	}
    }

    /**
     * Decode the bytes back into a Paxos Message.
     */
    class PaxosMessageDecoder extends SimpleChannelHandler {
	@Override public void messageReceived(ChannelHandlerContext ctx,
					      MessageEvent e) {
	    ChannelBuffer buf;
	    PaxosMessage msg;

	    buf = (ChannelBuffer)e.getMessage();

	    // Read in the message string. 
	    msg = new PaxosMessage();
	    msg.unSerialize(buf.array());

	    // Send the message upstream to the server handler.
	    Channels.fireMessageReceived(ctx, msg);
	}
    }

    /**
     * Encode the message. 
     */
    class PaxosMessageEncoder extends SimpleChannelHandler {
	@Override public void writeRequested(ChannelHandlerContext ctx,
					     MessageEvent e) {
	    PaxosMessage msg;
	    ChannelBuffer sendBuf;
	    byte[] data;

	    msg = (PaxosMessage)e.getMessage(); // The original message.

	    // Serialize the message.
	    data = msg.serialize();
	    sendBuf = ChannelBuffers.buffer(data.length);
	    sendBuf.writeBytes(data); // Write the actual msg.

	    // Send the message upstream.         
	    Channels.write(ctx, e.getFuture(), sendBuf);
	}
    }
}