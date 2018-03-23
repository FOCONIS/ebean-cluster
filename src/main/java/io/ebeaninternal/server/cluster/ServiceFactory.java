package io.ebeaninternal.server.cluster;

import io.ebean.config.ContainerConfig;
import io.ebeaninternal.server.cluster.socket.SocketClusterAutoDiscoveryBroadcast;
import io.ebeaninternal.server.cluster.socket.SocketClusterBroadcast;

/**
 * Factory for creating the ClusterBroadcast service.
 */
public class ServiceFactory implements ClusterBroadcastFactory {

  @Override
  public ClusterBroadcast create(ClusterManager manager, ContainerConfig containerConfig) {

    SocketConfig config = new SocketConfig();
    config.loadFromProperties(containerConfig.getProperties());

    if (config.isAutoDiscovery()) {
      return new SocketClusterAutoDiscoveryBroadcast(manager, config);
    } else {
      return new SocketClusterBroadcast(manager, config);
    }
  }

}
