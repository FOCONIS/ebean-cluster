package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.ClusterBroadcastConfig;
import io.ebeaninternal.server.cluster.broadcast.BroadcastListener;
import io.ebeaninternal.server.cluster.broadcast.BroadcastMessage;
import io.ebeaninternal.server.cluster.broadcast.Broadcaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * Broadcast messages across the cluster using sockets.
 */
public class SocketClusterAutoDiscoveryBroadcast extends SocketClusterBroadcast {

  private static final Logger logger = LoggerFactory.getLogger(SocketClusterAutoDiscoveryBroadcast.class);

  private final InetSocketAddress discoveryAddresss;

  private final int discoveryInterval;

  private BroadcastMessage broadcastMessage;

  private BroadcastListener listener;

  private Broadcaster broadcaster;


  public SocketClusterAutoDiscoveryBroadcast(ClusterManager manager, ClusterBroadcastConfig config) {
    super(manager, config);
    this.discoveryAddresss = parseFullName(config.getDiscoveryHostPort());
    this.discoveryInterval = config.getDiscoveryInterval();
  }

  /**
   * Tries to find the correct network interface.
   */
  @Override
  protected SocketClient configureSocketClient(ClusterBroadcastConfig config) {
    String localHostPort = config.getLocalHostPort();
    InetSocketAddress addr = parseFullName(localHostPort);



    InetAddress inetAddress = getInetAddress(config.getDiscoveryNet());
    if (inetAddress == null) {
      clusterLogger.warn("No interface found for subnet '{}'", config.getDiscoveryNet());
    } else {
      addr = new InetSocketAddress(inetAddress, addr.getPort());
      this.broadcastMessage = new BroadcastMessage(config.getDiscoveryGroup(),
          addr.getAddress().getHostAddress(),
          addr.getPort());
    }
    return new SocketClient(addr);

  }

  /**
   * Parses the IPv4 address. (Sorry no IPv6 yet)
   */
  private int parseIpV4(String ip) {
    try {
      int prefix = Integer.parseInt(ip);
      if (prefix >= 0 && prefix <= 32) {
        return 0xffffffff << (32 - prefix);
      }
    } catch (NumberFormatException nfe) {}

    try {
      return addrToInt(Inet4Address.getByName(ip));
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private int addrToInt( InetAddress addr) {
    byte[] b = addr.getAddress();
    return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
  }

  /**
   * Returns the host address of the network card that starts with <code>ipPrefix</code>
   */
  private InetAddress getInetAddress(String net) {
    if ("autodetect".equals(net)) {
      return getActiveInetAddress();
    } else {
      String[] tmp = net.split("\\/");
      int subnet = parseIpV4(tmp[0]);
      int mask = parseIpV4(tmp[1]);
      return getInetAddress(subnet, mask);
    }
  }
  private InetAddress getInetAddress(int subnet, int mask) {
    try {
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics.hasMoreElements()) {
        NetworkInterface nic = nics.nextElement();
        Enumeration<InetAddress> nicIps = nic.getInetAddresses();
        while (nicIps.hasMoreElements()) {
          InetAddress nicIp = nicIps.nextElement();
          int ipAsInt = addrToInt(nicIp);
          if (( ipAsInt & mask) == subnet) {
            clusterLogger.info("Netmask[{}]: IP {} is used for auto-discovery - {}", nicIp.getHostAddress(), nic.getDisplayName());
            return nicIp;
          }
        }
      }
    } catch (SocketException e) {
      logger.error("Error while searching for host address", e);
    }
    return null;
  }
  private InetAddress getActiveInetAddress() {
    NetworkInterface ret = null;
    try {
      final Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
      while (e.hasMoreElements()) {
        NetworkInterface nic = e.nextElement();
        logger.trace("Interface {} [up:{}, lo:{}, virt:{}] {}",
            nic.getName(), nic.isUp(), nic.isLoopback(), nic.isVirtual(), nic.getDisplayName());
        if (nic.isUp()) {
          if (ret == null) {
            ret = nic; // take first interface, that is up
          } else if (ret.isLoopback() && !nic.isLoopback()) {
            ret = nic; // prefer non loopback interfaces
          }else if (ret.isVirtual() && !nic.isVirtual()) {
            ret = nic; // prefer non virtual interfaces
          }
        }
      }
    } catch (SocketException e) {
      logger.error("Error while searching for active interface", e);
    }
    if (ret != null) {
      Enumeration<InetAddress> ips = ret.getInetAddresses();
      while (ips.hasMoreElements()) {
        InetAddress nicIp = ips.nextElement();
        clusterLogger.info("Autodetect: IP {} is used for auto-discovery - {}", nicIp.getHostAddress(), ret.getDisplayName());
        return nicIp;
      }
    }
    return null;
  }

  @Override
  public void startup() {
    super.startup();

    if (broadcastMessage != null) {
      try {
        listener = new BroadcastListener(discoveryAddresss, broadcastMessage, this);
        listener.startListening();
      } catch (IOException e) {
        logger.error("Error starting the discovery BroadcastListener", e);
      }
      try {
        broadcaster = new Broadcaster(discoveryAddresss, discoveryInterval, broadcastMessage);
        broadcaster.startBroadcasting();
      } catch (IOException e) {
        logger.error("Error starting the discovery broadcaster", e);
      }
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();

    if (broadcaster != null) {
      broadcaster.shutdown();    }

    if (listener != null) {
      listener.shutdown();
    }
  }


}
