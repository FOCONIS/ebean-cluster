package io.ebeaninternal.server.cluster.broadcast;

import io.ebeaninternal.server.cluster.socket.SocketClusterAutoDiscoveryBroadcast;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very simple broadcast listener
 *
 * A message is an ip with port e.g. 10.0.0.100:55500 and hostGroup.
 * If that host is unknown and matches to the host group, it will be added to the knownhosts
 */
public class BroadcastHandler  {

  private static final Logger logger = LoggerFactory.getLogger(BroadcastHandler.class);

  /**
   * The broadcasting thread.
   */
  private final Thread rxThread;

  private final Thread txThread;

  private final int txInterval;

  /**
   * The multicast socket.
   */
  private final MulticastSocket broadcastSocket;

  /**
   * shutting down flag.
   */
  private volatile boolean doingShutdown;

  private final SocketClusterAutoDiscoveryBroadcast scb;

  private final BroadcastMessage message;

  private final DatagramPacket packet;
;

  public BroadcastHandler(InetSocketAddress address, BroadcastMessage message, int txInterval, SocketClusterAutoDiscoveryBroadcast scb)
      throws IOException {

    this.broadcastSocket = new MulticastSocket(address.getPort());
    broadcastSocket.joinGroup(address.getAddress());

    this.scb = scb;

    this.message = message;
    byte[] buf = message.getBytes();
    this.packet = new DatagramPacket(buf, buf.length, address);

    this.rxThread = new Thread(this::rxTask, "EbeanClusterBroadcastRX");
    this.txThread = new Thread(this::txTask, "EbeanClusterBroadcastTX");
    this.txInterval = txInterval;


  }

  private void addMember(BroadcastMessage member, InetAddress sender) throws IOException {
    if (!message.getDiscoveryGroup().equals(member.getDiscoveryGroup())) {
      logger.debug("Broadcast message '{}' not for discoveryGroup '{}'", member, message.getDiscoveryGroup());
    } else if (message.getHostUuid().equals(member.getHostUuid())) { // This is a packet from this instance
      logger.trace("skip message from myself", member);
    } else if (scb.addMember(sender.getHostAddress(), member.getClusterPort())) {
      logger.info("Broadcast message '{}' processed successfully", member);
    } else {
      logger.trace("Broadcast message '{}' already processed", member);
    }
  }

  /**
   * Runs the RX loop, until shutdown is called.
   */
  private void rxTask() {
    // run in loop until doingShutdown is true...
    byte[] buf = new byte[512];
    DatagramPacket packet = new DatagramPacket(buf, buf.length);

    while (!doingShutdown) {
      InetAddress sender = null;
      try {
        broadcastSocket.receive(packet);
        sender = packet.getAddress();
        BroadcastMessage message = new BroadcastMessage(buf);
        logger.trace("RX: '{}' <- {}:{}", message, sender.getHostAddress(),packet.getPort());
        addMember(message, sender);
      } catch (SocketException e) {
        if (doingShutdown) {
          logger.debug("doingShutdown and accept threw:" + e.getMessage());
        } else {
          logger.error("Error while listening", e);
        }
      } catch (StreamCorruptedException e) {
        // ignore malformed packages.
        logger.debug("Malformed package from {}", sender, e);

      } catch (IOException e) {
        // log it and continue in the loop...
        logger.error("IOException processing message from {}", sender, e);
      }
    }
  }

  /**
   * Runs the TX loop, until shutdown is called.
   */
  private void txTask() {
    String mcastAddress = packet.getAddress().getHostAddress();
    while (!doingShutdown) {
      try {
        broadcastSocket.send(packet);
        logger.trace("TX: '{}' -> {}:{}", message, mcastAddress, packet.getPort());
        Thread.sleep(txInterval);
      } catch (SocketException e) {
        if (doingShutdown) {
          logger.debug("doingShutdown and accept threw: {}", e.getMessage());
        } else {
          logger.error("Error while listening", e);
        }
      } catch (InterruptedException e) {
        // ignore this exception
        logger.debug("Possibly expected due stopBroadcast? {}", e.getMessage());

      } catch (IOException e) {
        // log it and continue in the loop...
        logger.error("IOException processing cluster message", e);
      }
    }
  }

  /**
   * Start broadcasting.
   */
  public void start() {
    logger.trace("... startListening()");
    this.txThread.setDaemon(true);
    this.txThread.start();
    this.rxThread.setDaemon(true);
    this.rxThread.start();
  }

  /**
   * Shutdown this listener.
   */
  public void shutdown() {
    doingShutdown = true;

    rxThread.interrupt();
    txThread.interrupt();
    broadcastSocket.close();

    try {
      rxThread.join();
      txThread.join();
    } catch (InterruptedException ie) {
      // OK to ignore as expected to Interrupt for shutdown.
    }
  }
}
