package io.ebeaninternal.server.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configuration for clustering using TCP sockets.
 */
public class SocketConfig {

  /**
   * This local server in host:port format.
   */
  private String localHostPort = "127.0.0.1:9942";

  /**
   * All the cluster members in host:port format.
   */
  private List<String> members = new ArrayList<String>();

  private String threadPoolName = "EbeanCluster";

  private Properties properties;

  private boolean isAutoDiscovery = false;

  private String subDomain = "";

  private int clusterPort = 4499;

  public boolean isAutoDiscovery() {
    return isAutoDiscovery;
  }

  public String getSubDomain() {
    return subDomain;
  }

    public int getClusterPort() {
        return clusterPort;
    }

    /**
   * Return the host and port for this server instance.
   */
  public String getLocalHostPort() {
    return localHostPort;
  }

  /**
   * Set the host and port for this server instance.
   */
  public void setLocalHostPort(String localHostPort) {
    this.localHostPort = localHostPort;
  }

  /**
   * Return all the host and port for all the members of the cluster.
   */
  public List<String> getMembers() {
    return members;
  }

  /**
   * Set all the host and port for all the members of the cluster.
   */
  public void setMembers(List<String> members) {
    this.members = members;
  }

  /**
   * Return the thread pool name.
   */
  public String getThreadPoolName() {
    return threadPoolName;
  }

  /**
   * Set the thread pool name.
   */
  public void setThreadPoolName(String threadPoolName) {
    this.threadPoolName = threadPoolName;
  }

  /**
   * Load the properties into the configuration.
   */
  public void loadFromProperties(Properties properties) {

    this.properties = properties;
    this.threadPoolName = getProperty("ebean.cluster.threadPoolName", threadPoolName);
    this.localHostPort = getProperty("ebean.cluster.localHostPort", localHostPort);
    this.isAutoDiscovery = "auto".equals(getProperty("ebean.cluster.discovery", ""));
    this.subDomain = getProperty("ebean.cluster.discovery.domain", "172");
    this.clusterPort = Integer.parseInt(getProperty("ebean.cluster.discovery.port", "" + this.clusterPort));

    if (!isAutoDiscovery) {
      String rawMembers = getProperty("ebean.cluster.members", "");
      String[] split = rawMembers.split("[,;]");
      for (String rawMember : split) {
          if (!rawMember.trim().isEmpty()) {
              members.add(rawMember.trim());
          }
      }
    }
  }

  private String getProperty(String key, String defaultValue) {
    String value = properties.getProperty(key.toLowerCase());
    if (value != null) {
      return value.trim();
    }
    value = properties.getProperty(key, defaultValue);
    return (value == null) ? defaultValue : value.trim();
  }
}
