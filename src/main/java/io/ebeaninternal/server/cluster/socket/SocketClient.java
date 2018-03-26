package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.message.ClusterMessage;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The client side of the socket clustering.
 */
public class SocketClient {

  private static final Logger logger = LoggerFactory.getLogger(SocketClient.class);

  private final InetSocketAddress address;

  private final String name;

  private final boolean local;

  /**
   * The local port where we expect the response.
   */
  private final int localPort;

  /**
   * lock guarding all access
   */
  private final ReentrantLock lock;

  private final BlockingQueue<Object> pingQueue = new ArrayBlockingQueue(1);

  private Socket socket;

  private OutputStream os;

  private DataOutputStream dataOutput;

  private boolean dynamicMember;

  private boolean online;

  /**
   * Construct with an IP address and port.
   */
  SocketClient(InetSocketAddress address, int localPort, String name) {
    this.lock = new ReentrantLock(false);
    this.address = address;
    this.name = name;
    this.localPort = localPort;
    this.local = address.getPort() == localPort && ownAddress();
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }

  public boolean isOnline() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      return online;
    } finally {
      lock.unlock();
    }
  }

  // void setOnline(boolean online) throws IOException {
  // final ReentrantLock lock = this.lock;
  // lock.lock();
  // try {
  // this.online = online;
  // if (online) {
  // connect();
  // } else {
  // disconnect();
  // }
  // } finally {
  // lock.unlock();
  // }
  // }

  void setOffline() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      this.online = false;
      disconnect();
    } finally {
      lock.unlock();
    }
  }

  private void reconnect() throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (online) {
        disconnect();
        connect();
      }
    } finally {
      lock.unlock();
    }
  }

  private void disconnect() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          logger.info("Error disconnecting from Cluster member {}:{}", address.getAddress().getHostAddress(),
              address.getPort(), e);
        }
        os = null;
        dataOutput = null;
        socket = null;
      }
    } finally {
      lock.unlock();
    }
  }

  boolean register(boolean force) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (!force && online) {
        return false;
      }
      try {
        online = true;
        send(ClusterMessage.register(localPort));
        return true;
      } catch (IOException e) {
        disconnect();
        online = false;
        return false;
      }
    } finally {
      lock.unlock();
    }
  }

  void send(ClusterMessage msg) throws IOException {
    if (online) {
      logger.debug("SEND -> {}:{}; {}", address.getAddress().getHostAddress(), address.getPort(), msg);
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
        if (dataOutput == null) {
          connect();
        }
        try {
          msg.write(dataOutput);
        } catch (IOException e) {
          reconnect();
          logger.info("RETRY -> {}:{}; {}", address.getAddress().getHostAddress(), address.getPort(), msg);
          msg.write(dataOutput);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private void connect() throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (socket == null) {
        Socket s = new Socket();
        s.setKeepAlive(true);
        s.connect(address);

        this.socket = s;
        this.os = socket.getOutputStream();
        this.dataOutput = new DataOutputStream(os);
      }
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
          while (addresses.hasMoreElements()) {
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

  /**
   * The ping queue should be empty when sending a ping and should be filled, when
   * the pong arrives.
   */
  public BlockingQueue<Object> getPingQueue() {
    return pingQueue;
  }

  public boolean isDynamicMember() {
    return dynamicMember;
  }

  public void setDynamicMember(boolean dynamicMember) {
    this.dynamicMember = dynamicMember;
  }
}
