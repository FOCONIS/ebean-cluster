package io.ebeaninternal.server.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configuration for clustering using TCP sockets.
 *
 * <ul>
 * <li><code>ebean.cluster.localHostPort</code> (Default:
 * <code>127.0.0.1:9942</code>) the local interface we bind to. If
 * auto-discovery is enabled, the IP is ignored, only the port is used</li>
 *
 * <li><code>ebean.cluster.discovery.enabled</code> (Default: false) controls,
 * if auto-discovery is enabled.</li>
 *
 * <li><code>ebean.cluster.discovery.net</code> (172.0.0.0/12) The network,
 * where auto discovery runs. The ClusterManager tries to auto detect the
 * interface.</li>
 *
 * <li><code>ebean.cluster.discovery.hostPort</code> (Default: 224.0.0.180:4446)
 * The Multicast address that is used to communicate with other instances. The
 * address must be the same on all instances</li>
 *
 * <li><code>ebean.cluster.discovery.group</code> (Default: ebean-default) The
 * multicast group name. The group must be the same on all instances</li>
 *
 * <li><code>ebean.cluster.discovery.interval</code> (Default 30000) the
 * interval in milliseconds to send broadcast messages. 0 = disabled.</li>
 *
 * </ul>
 *
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

  private String discoveryNet = "172.0.0.0/12";

  private String discoveryHostPort = "224.0.0.180:4446";

  private String discoveryGroup = "ebean-default";

  private int discoveryInterval = 30000;

  public boolean isAutoDiscovery() {
    return isAutoDiscovery;
  }

  /**
   * Sets the discovery group. Only servers on same discovery group will find each other.
   */
  public void setDiscoveryGroup(String discoveryGroup) {
    this.discoveryGroup = discoveryGroup;
  }

  /**
   * Returns the discovery group.
   */
  public String getDiscoveryGroup() {
    return discoveryGroup;
  }

  /**
   * Sets the discovery host and port (MCast addres). E.g 224.0.0.180:4446
   */
  public void setDiscoveryHostPort(String discoveryHostPort) {
    this.discoveryHostPort = discoveryHostPort;
  }

  /**
   * Returns the discovery host and port
   */
  public String getDiscoveryHostPort() {
    return discoveryHostPort;
  }

  /**
   * Sets the discovery network (ip with netmask). All cluster partner must be on the same subnet.
   */
  public void setDiscoveryNet(String discoveryNet) {
    this.discoveryNet = discoveryNet;
  }

  /**
   * Returns the discovery net.
   */
  public String getDiscoveryNet() {
    return discoveryNet;
  }

  /**
   * Sets the discovery interval in milliseconds. Setting to 0 means, no discovery broadcast is sent.
   */
  public void setDiscoveryInterval(int discoveryInterval) {
    this.discoveryInterval = discoveryInterval;
  }

  /**
   * Returns the discovery interval in milliseconds.
   */
  public int getDiscoveryInterval() {
    return discoveryInterval;
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

    this.isAutoDiscovery = "true".equals(getProperty("ebean.cluster.discovery.enabled", String.valueOf(isAutoDiscovery)));

    this.discoveryGroup = getProperty("ebean.cluster.discovery.group", discoveryGroup);
    this.discoveryNet = getProperty("ebean.cluster.discovery.net", discoveryNet);
    this.discoveryHostPort = getProperty("ebean.cluster.discovery.hostPort", discoveryHostPort);
    this.discoveryInterval = Integer.parseInt(getProperty("ebean.cluster.discovery.interval", String.valueOf(discoveryInterval)));

    // add static members (also when discovery is active)
    String rawMembers = getProperty("ebean.cluster.members", "");
    String[] split = rawMembers.split("[,;]");
    for (String rawMember : split) {
      if (!rawMember.trim().isEmpty()) {
        members.add(rawMember.trim());
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
