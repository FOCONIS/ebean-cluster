package io.ebeaninternal.server.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configuration for clustering using TCP sockets.
 *
 * <ul>
 * <li><code>ebean.cluster.port</code> The TCP port to use for cluster messages.
 * 0 means a random value between <code>ebean.cluster.portLow</code> and
 * <code>ebean.cluster.portHigh</code> is used. (default)</li>
 *
 * <li><code>ebean.cluster.bindAddr</code> The bind address for incoming TCP
 * cluster messages.</li>
 *
 * <li><code>ebean.cluster.members</code> A comma separated list of cluster
 * members.</li>
 *
 * <li><code>ebean.cluster.discovery.mode</code> (Default: static, possible
 * values off,static,autodiscovery) controls, if cluster transport and
 * auto-discovery is enabled.</li>
 *
 * <li><code>ebean.cluster.discovery.multicast</code> (Default:
 * 224.0.0.180:4446) The Multicast address that is used to communicate with
 * other instances. The address must be the same on all instances</li>
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
public class ClusterBroadcastConfig {

  public enum Mode {
    /**
     * The cluster transport is off. No listeners are started,
     */
    OFF,

    /**
     * The cluster transport is in static mode. All members must be specified or added manually.
     */
    STATIC,

    /**
     * The cluster transport is in auto discovery mode. You can also add static members or add them manually.
     */
    AUTODISCOVERY
  }

  /**
   * The bind addres, where to listen for incoming packets.
   */
  private String bindAddr = "0.0.0.0";

  /**
   * The TCP/IP port to listen for incoming cluster messages.
   */
  private int port = 0;

  // port range for random ports
  private int portLow = 50000;
  private int portHigh = 60000;

  private String multicast = "224.0.0.180:4446";

  private String discoveryGroup = "ebean-default";

  private int discoveryInterval = 30000;

  /**
   * All the cluster members in host:port format.
   */
  private List<String> members = new ArrayList<String>();

  private String threadPoolName = "EbeanCluster";

  private Properties properties;

  private Mode mode = Mode.STATIC;

  /**
   * enables or disables the autodiscovery feature.
   */
  public void setMode(Mode mode) {
    this.mode = mode;
  }

  /**
   * Returns <code>true</code> if AutoDiscovery is active.
   */
  public Mode getMode() {
    return mode;
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
   * Sets the multicast host and port for UDP broadcasts. E.g 224.0.0.180:4446
   */
  public void setMulticast(String multicast) {
    this.multicast = multicast;
  }

  /**
   * @return the multicast
   */
  public String getMulticast() {
    return multicast;
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
   * returns the bind address.
   */
  public String getBindAddr() {
    return bindAddr;
  }

  /**
   * Sets the bind address.
   */
  public void setBindAddr(String bindAddr) {
    this.bindAddr = bindAddr;
  }

  /**
   * Return the host and port for this server instance.
   */
  public int getPort() {
    return port;
  }

  /**
   * Set the TCP port for this server instance.
   */
  public void setPort(int port) {
    this.port = port;
  }

  public int getPortLow() {
    return portLow;
  }

  public void setPortLow(int portLow) {
    this.portLow = portLow;
  }

  public int getPortHigh() {
    return portHigh;
  }

  public void setPortHigh(int portHigh) {
    this.portHigh = portHigh;
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
    this.bindAddr = getProperty("ebean.cluster.bindAddr", bindAddr);
    this.port = getProperty("ebean.cluster.port", port);
    this.portLow = getProperty("ebean.cluster.port.low", portLow);
    this.portHigh = getProperty("ebean.cluster.port.high", portHigh);

    this.mode = Mode.valueOf(getProperty("ebean.cluster.mode", mode.name()).toUpperCase());

    this.discoveryGroup = getProperty("ebean.cluster.discovery.group", discoveryGroup);
    this.multicast = getProperty("ebean.cluster.discovery.multicast", multicast);
    this.discoveryInterval = getProperty("ebean.cluster.discovery.interval", discoveryInterval);

    // add static members (also when discovery is active)
    String rawMembers = getProperty("ebean.cluster.members", "");
    String[] split = rawMembers.split("[,;]+");
    for (String rawMember : split) {
      if (!rawMember.isEmpty()) {
        members.add(rawMember);
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

  private int getProperty(String key, int defaultValue) {
    String value = properties.getProperty(key.toLowerCase());
    if (value != null) {
      return Integer.parseInt(value.trim());
    }
    value = properties.getProperty(key, String.valueOf(defaultValue));
    return (value == null) ? defaultValue : Integer.parseInt(value.trim());
  }
}
