package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.message.ClusterMessage;
import io.ebeaninternal.server.cluster.message.ClusterMessage.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.util.Enumeration;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The client side of the socket clustering.
 */
class SocketClient {

  private static final Logger logger = LoggerFactory.getLogger(SocketClient.class);

  private final InetSocketAddress address;

  private final String hostPort;

  private final boolean local;

  /**
   * The local port where we expect the response.
   */
  private final int localPort;

  /**
   * lock guarding all access
   */
  private final ReentrantLock lock;

  private boolean online;

  private Socket socket;

  private OutputStream os;

  private DataOutputStream dataOutput;

  private int pings;

  private int pongs;




  /**
   * Construct with an IP address and port.
   */
  SocketClient(InetSocketAddress address, int localPort) {
    this.lock = new ReentrantLock(false);
    this.address = address;
    this.hostPort = address.getAddress().getHostAddress() + ":" + address.getPort();
    this.localPort = localPort;
    this.local = address.getPort() == localPort && ownAddress();
  }

  public String xgetHostPort() {
    return hostPort;
  }

  @Override
  public String toString() {
    return hostPort;
  }

  boolean isOnline() {
    return online;
  }

  void setOnline(boolean online) throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (online) {
        setOnline();
      } else {
        disconnect();
      }
    } finally {
      lock.unlock();
    }
  }

  void reconnect() throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      disconnect();
      connect();
    } finally {
      lock.unlock();
    }
  }

  void disconnect() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      this.online = false;
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          logger.info("Error disconnecting from Cluster member {}:{}",
              address.getAddress().getHostAddress(), address.getPort(), e);
        }
        os = null;
        dataOutput = null;
        socket = null;
      }
    } finally {
      lock.unlock();
    }
  }

  boolean register() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      try {
        setOnline();
        send(ClusterMessage.register(localPort));
        return true;
      } catch (IOException e) {
        disconnect();
        return false;
      }
    } finally {
      lock.unlock();
    }
  }

  void send(ClusterMessage msg) throws IOException {
    logger.debug("SEND -> {}:{}; {}", address.getAddress().getHostAddress(), address.getPort(), msg);
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (online) {
        if (msg.getType() == Type.PING) {
          pings++;
        }
        msg.write(dataOutput);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set whether the client is thought to be online.
   */
  private void setOnline() throws IOException {
    if (!online) {
      connect();
      this.online = true;
    }
  }

  private void connect() throws IOException {
    if (socket != null) {
      throw new IllegalStateException("Already got a socket connection?");
    }
    Socket s = new Socket();
    s.setKeepAlive(true);
    s.connect(address);

    this.socket = s;
    this.os = socket.getOutputStream();
    this.dataOutput = new DataOutputStream(os);
  }

  /**
   * @param message
   */
  public void processPong(ClusterMessage message) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      pongs++;
      long latency = System.currentTimeMillis() - message.getTimestamp();
      logger.trace("PING: sent {}, recv {}, latency {} ms", pings, pongs, latency);
    } finally {
      lock.unlock();
    }

  }

  private boolean ownAddress() {
    try {
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics.hasMoreElements()) {
        NetworkInterface nic = nics.nextElement();
        if (!nic.isLoopback() && !nic.isVirtual()) {
          Enumeration<InetAddress> addresses = nic.getInetAddresses();
          while (addresses.hasMoreElements()){
              InetAddress ip = addresses.nextElement();
              if (ip.equals(address.getAddress())) {
                return true;
              }
          }
        }
      }
    } catch (IOException e) {
      logger.error("Error while iterating over IP addresses");
    }
    return false;
  }

  /**
   * Checks if this is our local address.
   */
  public boolean isLocal() {
    return local;
  }

  /**
   * Returns the port of this client.
   */
  public int getPort() {
    return address.getPort();
  }

}
