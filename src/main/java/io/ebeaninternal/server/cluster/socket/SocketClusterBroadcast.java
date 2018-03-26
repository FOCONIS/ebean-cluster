package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterBroadcast;
import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.ClusterBroadcastConfig;
import io.ebeaninternal.server.cluster.ClusterBroadcastConfig.Mode;
import io.ebeaninternal.server.cluster.broadcast.BroadcastHandler;
import io.ebeaninternal.server.cluster.broadcast.BroadcastMessage;
import io.ebeaninternal.server.cluster.message.ClusterMessage;
import io.ebeaninternal.server.cluster.message.ClusterMessage.Type;
import io.ebeaninternal.server.cluster.message.MessageReadWrite;
import io.ebeaninternal.server.transaction.RemoteTransactionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast messages across the cluster using sockets.
 */
public class SocketClusterBroadcast implements ClusterBroadcast {

  private static final Logger clusterLogger = LoggerFactory.getLogger("io.ebean.Cluster");

  private static final Logger logger = LoggerFactory.getLogger(SocketClusterBroadcast.class);

  private final Map<String, SocketClient> clientMap = new ConcurrentHashMap<>();

  private final SocketClusterListener listener;

  private final MessageReadWrite messageReadWrite;

  private final BroadcastHandler broadcastHandler;

  private final AtomicLong countOutgoing = new AtomicLong();

  private final AtomicLong countIncoming = new AtomicLong();

  private int localPort;



  public SocketClusterBroadcast(ClusterManager manager, ClusterBroadcastConfig config)   {

    this.messageReadWrite = new MessageReadWrite(manager);

    if (config.getMode() == Mode.OFF) {
      clusterLogger.info("Clustering is disabled. Not starting any cluster transport");
      this.listener = null;
      this.broadcastHandler = null;
    } else {
      try {
        this.localPort = config.getPort();
        if (this.localPort != 0) {
          this.listener = createListener(config, localPort);
        } else {
          this.listener = createListenerOnRandomPort(config);
        }

        List<String> members = config.getMembers();

        clusterLogger.info("Clustering members[{}], cluster-port {}, mode: {}", members, this.localPort,
            config.getMode());

        for (String memberHostPort : members) {
          SocketClient member = createMember(memberHostPort);
          if (member.isLocal()) {
            logger.info("Ignoring local member: {}", member);
          } else {
            clientMap.put(memberHostPort, member);
          }
        }
        // setup the autodiscovery broadcaster.
        if (config.getMode() == Mode.AUTODISCOVERY) {
          this.broadcastHandler = createBroadcastHandler(config, localPort);
        } else {
          this.broadcastHandler = null;
        }
      } catch (IOException ex) {
        throw new RuntimeException("Error creating SocketClusterBroadcast", ex);
      }
    }
  }

  private SocketClient createMember(String memberHostPort) {
    return new SocketClient(parseFullName(memberHostPort), localPort, memberHostPort);
  }

  /**
   * Tries to find an open port and creates a listener on it.
   */
  private SocketClusterListener createListenerOnRandomPort(ClusterBroadcastConfig config) throws IOException {
    int i = 0;
    while (true) {
      try {
        int rnd = (int) ((config.getPortHigh() - config.getPortLow()) * Math.random());
        this.localPort = config.getPortLow() + rnd;
        return createListener(config, this.localPort);
      } catch (BindException be) {
        logger.trace("Port {} already in use", this.localPort, be);
        if (i++ > 10) {
          throw be;
        }
      }
    }
  }

  private SocketClusterListener createListener(ClusterBroadcastConfig config, int port) throws IOException {
    InetSocketAddress inetSocket = new InetSocketAddress(config.getBindAddr(), localPort);
    return new SocketClusterListener(this, inetSocket, config.getThreadPoolName());
  }

  /**
   * create the auto discovery broadcast handler.
   */
  private BroadcastHandler createBroadcastHandler(ClusterBroadcastConfig config, int localPort) throws IOException {
    InetSocketAddress discoveryAddresss = parseFullName(config.getMulticast());
    BroadcastMessage broadcastMessage = new BroadcastMessage(config.getDiscoveryGroup(), UUID.randomUUID(), localPort);
    return new BroadcastHandler(discoveryAddresss, broadcastMessage, config.getDiscoveryInterval(), this);
  }


  /**
   * Return the current status of this instance.
   */
  public SocketClusterStatus getStatus() {

    // count of online members
    int currentGroupSize = 0;
    for (SocketClient member : clientMap.values()) {
      if (member.isOnline()) {
        ++currentGroupSize;
      }
    }

    long txnIn = countIncoming.get();
    long txnOut = countOutgoing.get();

    return new SocketClusterStatus(currentGroupSize, txnIn, txnOut);
  }

  @Override
  public void startup() {
    if (listener != null) {
      listener.startListening();
      register();
      if (broadcastHandler != null) {
        broadcastHandler.start();
      }
    }
  }

  @Override
  public void shutdown() {
    if (listener != null) {
      if (broadcastHandler != null) {
        broadcastHandler.shutdown();
      }
      deregister();
      listener.shutdown();
    }
  }

  /**
   * Register with all the other members of the Cluster.
   */
  private void register() {
    for (SocketClient member : clientMap.values()) {
      member.register(true);
      clusterLogger.info("Register as online with member [{}]", member);
    }
  }

  private void send(SocketClient client, ClusterMessage msg) {

    try {
      // alternative would be to connect/disconnect here but prefer to use keep alive
      logger.trace("... send to member {} broadcast msg: {}", client, msg);
      client.send(msg);
    } catch (Exception ex) {
      if (client.isDynamicMember()) {
        client.setOffline();
        clusterLogger.warn("Taking client {} offline. Error sending message {}", client, msg, ex);
      } else {
        clusterLogger.error("Error sending message {}", msg, ex);
      }

    }
  }

  public SocketClient registerMember(String address, int port) throws IOException {
    return registerMember(address + ":" + port);
  }

  public SocketClient registerMember(String fullName) throws IOException {
    SocketClient member = clientMap.computeIfAbsent(fullName, this::createMember);
    if (member.register(false)) {
      clusterLogger.info("Cluster member [{}] set to online", fullName);
      return member;
    } else {
      return null;
    }
  }

  public void disconnectMember(String address, int port) throws IOException {
     disconnectMember(address + ":" + port);
  }

  public void disconnectMember(String fullName) throws IOException {
    SocketClient member = clientMap.get(fullName);
    if (member != null) {
      clusterLogger.info("Cluster member [{}] set to offline", fullName);
      member.setOffline();
    }
  }

  private void sendPong(String address, int port) throws IOException {
    String fullName = address + ":" + port;
    SocketClient member = clientMap.get(fullName);
    if (member != null) {
      ClusterMessage msg = ClusterMessage.pong(localPort);
      member.send(msg);
    }
  }

  private void processPong(String address, int port) throws IOException {
    String fullName = address + ":" + port;
    SocketClient member = clientMap.get(fullName);
    if (member != null) {
      member.getPingQueue().offer("");
    }
  }

  /**
   * Send the payload to all the members of the cluster.
   */
  @Override
  public void broadcast(RemoteTransactionEvent remoteTransEvent) {
    try {
      countOutgoing.incrementAndGet();
      byte[] data = messageReadWrite.write(remoteTransEvent);
      ClusterMessage msg = ClusterMessage.transEvent(data);
      broadcast(msg);
    } catch (Exception e) {
      logger.error("Error sending RemoteTransactionEvent " + remoteTransEvent + " to cluster members.", e);
    }
  }

  private void broadcast(ClusterMessage msg) {
    for (SocketClient member : clientMap.values()) {
      send(member, msg);
    }
  }

  public int ping(SocketClient member) {
    if (!member.isOnline()) {
      return -1;
    }
    try {
      ClusterMessage msg = ClusterMessage.ping(localPort);
      long start = System.currentTimeMillis();
      send(member, msg);
      if (member.getPingQueue().poll(2, TimeUnit.SECONDS) == null) {
        return -1;
      } else {
        return (int) (System.currentTimeMillis() - start);
      }
    } catch (InterruptedException e) {
      logger.error("Error sending Ping to cluster members.", e);
      return -1;
    }
  }

  /**
   * Leave the cluster.
   */
  private void deregister() {
    clusterLogger.info("Leaving cluster");
    ClusterMessage msg = ClusterMessage.deregister(localPort);
    broadcast(msg);
    for (SocketClient member : clientMap.values()) {
      member.setOffline();
    }
  }

  /**
   * Process an incoming Cluster message.
   *
   * Returns false, if the remote instance is shutting down.
   */
  boolean process(SocketConnection request) throws ClassNotFoundException {

    try {
      ClusterMessage message = ClusterMessage.read(request.getDataInputStream());
      logger.debug("RECV <- {}:{}; {}", request.getSourceAddress(), request.getSourcePort(), message);


      if (message.getType() == Type.TRANSACTION) {
        countIncoming.incrementAndGet();
        RemoteTransactionEvent transEvent = messageReadWrite.read(message.getData());
        transEvent.run();
        return false;

      } else {
        SocketClient member;

        switch (message.getType()) {
        case REGISTER:
          registerMember(request.getSourceAddress(), message.getPort());
          return false;

        case DEREGISTER:
          // we do not send a deregister message back.
          disconnectMember(request.getSourceAddress(), message.getPort());
          return true;

        case PING:
          sendPong(request.getSourceAddress(), message.getPort());
          return false;

        case PONG:
          processPong(request.getSourceAddress(), message.getPort());
          return false;

        default:
          return true;
        }
      }

    } catch (InterruptedIOException e) {
      logger.info("Timeout waiting for message", e);
      try {
        request.disconnect();
      } catch (IOException ex) {
        logger.info("Error disconnecting after timeout", ex);
      }
      return true;

    } catch (EOFException e) {
      logger.debug("EOF disconnecting");
      return true;
    } catch (IOException e) {
      logger.info("IO Error waiting/reading message", e);
      return true;
    }
  }

  /**
   * Parse a host:port into a InetSocketAddress.
   */
  InetSocketAddress parseFullName(String hostAndPort) {

    try {
      hostAndPort = hostAndPort.trim();
      int colonPos = hostAndPort.indexOf(":");
      if (colonPos == -1) {
        String msg = "No colon \":\" in " + hostAndPort;
        throw new IllegalArgumentException(msg);
      }
      String host = hostAndPort.substring(0, colonPos);
      String sPort = hostAndPort.substring(colonPos + 1, hostAndPort.length());
      int port = Integer.parseInt(sPort);

      return new InetSocketAddress(host, port);

    } catch (Exception ex) {
      throw new RuntimeException("Error parsing [" + hostAndPort + "] for the form [host:port]", ex);
    }
  }

  public Collection<SocketClient> getMembers() {
    return clientMap.values();
  }

}
