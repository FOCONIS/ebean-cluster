package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.ClusterBroadcastConfig;
import io.ebeaninternal.server.cluster.broadcast.BroadcastHandler;
import io.ebeaninternal.server.cluster.broadcast.BroadcastMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broadcast messages across the cluster using sockets.
 */
public class SocketClusterAutoDiscoveryBroadcast extends SocketClusterBroadcast {

  private static final Logger logger = LoggerFactory.getLogger(SocketClusterAutoDiscoveryBroadcast.class);

  private final InetSocketAddress discoveryAddresss;

  private final int discoveryInterval;

  private BroadcastMessage broadcastMessage;

  private BroadcastHandler broadcastHandler;

  public SocketClusterAutoDiscoveryBroadcast(ClusterManager manager, ClusterBroadcastConfig config) {
    super(manager, config);
    this.discoveryAddresss = parseFullName(config.getMulticast());
    this.discoveryInterval = config.getDiscoveryInterval();
    this.broadcastMessage = new BroadcastMessage(config.getDiscoveryGroup(), UUID.randomUUID(), config.getPort());
  }

  @Override
  public void startup() {
    super.startup();

    if (broadcastMessage != null) {
      try {
        broadcastHandler = new BroadcastHandler(discoveryAddresss, broadcastMessage, discoveryInterval, this);
        broadcastHandler.start();
      } catch (IOException e) {
        logger.error("Error starting the discovery BroadcastListener", e);
      }
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (broadcastHandler != null) {
      broadcastHandler.shutdown();
    }
  }

}
