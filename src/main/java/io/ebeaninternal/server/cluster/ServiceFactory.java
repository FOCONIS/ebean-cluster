package io.ebeaninternal.server.cluster;

import java.net.BindException;

import io.ebean.config.ContainerConfig;
import io.ebeaninternal.server.cluster.socket.SocketClusterAutoDiscoveryBroadcast;
import io.ebeaninternal.server.cluster.socket.SocketClusterBroadcast;

/**
 * Factory for creating the ClusterBroadcast service.
 */
public class ServiceFactory implements ClusterBroadcastFactory {

  @Override
  public ClusterBroadcast create(ClusterManager manager, ContainerConfig containerConfig) {

    ClusterBroadcastConfig config = new ClusterBroadcastConfig();
    config.loadFromProperties(containerConfig.getProperties());

    if (config.isAutoDiscovery()) {
      int i = 0;
      RuntimeException error = null;
      while(i++ < 5) {
        if (config.getLocalHostPort().isEmpty()) {
          config.setLocalHostPort(":"+(int)(Math.random()*1000 + 50000));
        }

        try {
          return new SocketClusterAutoDiscoveryBroadcast(manager, config);
        } catch (RuntimeException e) {
          error = e;
          if (e.getCause() instanceof BindException) {
            config.setLocalHostPort("");
            SocketClusterBroadcast.clusterLogger.warn("Address {} already in use. Trying to use a random one",
                config.getLocalHostPort());
          } else {
            break;
          }
        }
      }
      throw error;
    } else {
      return new SocketClusterBroadcast(manager, config);
    }
  }

}
