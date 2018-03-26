package io.ebeaninternal.server.cluster;

import io.ebean.config.ContainerConfig;
import io.ebeaninternal.server.cluster.socket.SocketClusterBroadcast;

/**
 * Factory for creating the ClusterBroadcast service.
 */
public class ServiceFactory implements ClusterBroadcastFactory {

  @Override
  public ClusterBroadcast create(ClusterManager manager, ContainerConfig containerConfig) {

    ClusterBroadcastConfig config = new ClusterBroadcastConfig();
    config.loadFromProperties(containerConfig.getProperties());

    return new SocketClusterBroadcast(manager, config);
  }

}
