package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterBroadcast;
import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.ClusterBroadcastConfig;
import io.ebeaninternal.server.cluster.message.ClusterMessage;
import io.ebeaninternal.server.cluster.message.ClusterMessage.Type;
import io.ebeaninternal.server.cluster.message.MessageReadWrite;
import io.ebeaninternal.server.transaction.RemoteTransactionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast messages across the cluster using sockets.
 */
public class SocketClusterBroadcast implements ClusterBroadcast {

  public static final Logger clusterLogger = LoggerFactory.getLogger("io.ebean.Cluster");

  private static final Logger logger = LoggerFactory.getLogger(SocketClusterBroadcast.class);

  private final Map<String, SocketClient> clientMap = new ConcurrentHashMap<>();

  private final SocketClusterListener listener;

  private final MessageReadWrite messageReadWrite;

  private final int localPort;

  private final AtomicLong countOutgoing = new AtomicLong();

  private final AtomicLong countIncoming = new AtomicLong();


  public SocketClusterBroadcast(ClusterManager manager, ClusterBroadcastConfig config) {

    this.messageReadWrite = new MessageReadWrite(manager);

    List<String> members = config.getMembers();

    this.localPort = config.getPort();
    clusterLogger.info("Clustering members[{}], cluster-port {}, auto-discovery: {}",
        members, this.localPort, config.isAutoDiscovery());

    for (String memberHostPort : members) {
      InetSocketAddress member = parseFullName(memberHostPort);
      SocketClient client = new SocketClient(member, localPort);
      if (client.isLocal()) {
        logger.info("Ignoring local address: {}", member);
      } else {
        clientMap.put(client.xgetHostPort(), client);
      }
    }
    InetSocketAddress inetSocket = new InetSocketAddress(config.getBindAddr(), localPort);
    this.listener = new SocketClusterListener(this, inetSocket, config.getThreadPoolName());
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
    listener.startListening();
    register();
  }

  @Override
  public void shutdown() {
    deregister();
    listener.shutdown();
  }

  public boolean addMember(String hostIp, int clusterPort) throws IOException {
    return addMember(new SocketClient(new InetSocketAddress(hostIp, clusterPort), localPort));
  }

  boolean addMember(SocketClient member) throws IOException {
    if (clientMap.putIfAbsent(member.xgetHostPort(), member) == null) {
      // send register message back to the member. Use our address
      //ClusterMessage msg = ClusterMessage.register();
      setMemberOnline(member.xgetHostPort(), true);
      member.register();

      return true;
    } else {
      return false;
    }
  }

  /**
   * Register with all the other members of the Cluster.
   */
  private void register() {
    ClusterMessage msg = ClusterMessage.register(localPort);
    for (SocketClient member : clientMap.values()) {
      boolean online = member.register();
      clusterLogger.info("Register as online with member [{}]", member.xgetHostPort(), online);
    }
  }

  private void send(SocketClient client, ClusterMessage msg) {

    try {
      // alternative would be to connect/disconnect here but prefer to use keep alive
      logger.trace("... send to member {} broadcast msg: {}", client, msg);
      client.send(msg);

    } catch (Exception ex) {
      logger.error("Error sending message", ex);
      try {
        client.reconnect();
      } catch (IOException e) {
        logger.error("Error trying to reconnect", ex);
      }
    }
  }

  private void setMemberOnline(String fullName, boolean online) throws IOException {
    clusterLogger.info("Cluster member [{}] online[{}]", fullName, online);
    SocketClient member = clientMap.computeIfAbsent(fullName, key -> new SocketClient(parseFullName(key), localPort));
    member.setOnline(online);
    if (clusterLogger.isDebugEnabled()) {
      for (SocketClient m : clientMap.values()) {
        clusterLogger.debug("Member: {}, online: {}", m.xgetHostPort(), m.isOnline());
      }
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
    ping();
  }

  public void ping() {
    try {
      countOutgoing.incrementAndGet();
      ClusterMessage msg = ClusterMessage.ping(localPort);
      broadcast(msg);
    } catch (Exception e) {
      logger.error("Error sending Ping to cluster members.", e);
    }
  }

  private void broadcast(ClusterMessage msg) {
    for (SocketClient member : clientMap.values()) {
      send(member, msg);
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
      member.disconnect();
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
        return true;

      } else {
        String memberName = request.getSourceAddress()+":"+message.getPort();
        SocketClient member;

        switch (message.getType()) {
        case REGISTER:
          setMemberOnline(memberName, true);
          return false;

        case DEREGISTER:
          setMemberOnline(memberName, true);
          return true;

        case PING:
           member = clientMap.get(memberName);
          if (member != null) {
            member.send(message.pong(localPort));
          }
          return false;

        case PONG:
          member = clientMap.get(memberName);
          if (member != null) {
            member.processPong(message);;
          }
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

}
