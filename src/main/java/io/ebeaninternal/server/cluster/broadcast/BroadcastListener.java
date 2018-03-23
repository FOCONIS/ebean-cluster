package io.ebeaninternal.server.cluster.broadcast;

import io.ebeaninternal.server.cluster.socket.SocketClusterAutoDiscoveryBroadcast;

import java.io.IOException;
import java.net.DatagramPacket;
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
public class BroadcastListener implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(BroadcastListener.class);

  /**
   * The broadcasting thread.
   */
  private final Thread broadcastListeningThread;

  /**
   * The multicast socket.
   */
  private final MulticastSocket broadcastSocket;

  /**
   * shutting down flag.
   */
  private boolean doingShutdown;

  private final SocketClusterAutoDiscoveryBroadcast scb;
  //private final CopyOnWriteArraySet<BroadcastMessage> members = new CopyOnWriteArraySet<>();

  private final BroadcastMessage myMessage;

  public BroadcastListener(InetSocketAddress address, BroadcastMessage myMessage, SocketClusterAutoDiscoveryBroadcast scb)
      throws IOException {

    this.broadcastListeningThread = new Thread(this, "EbeanClusterBroadcastListener");
    this.broadcastSocket = new MulticastSocket(address.getPort());
    this.broadcastSocket.setSoTimeout(60000);

    broadcastSocket.joinGroup(address.getAddress());

    this.scb = scb;
    this.myMessage = myMessage;
  }

  private void addMember(BroadcastMessage member) throws IOException {
    if (!myMessage.getDiscoveryGroup().equals(member.getDiscoveryGroup())) {
      logger.debug("Broadcast message '{}' not for discoveryGroup '{}'", member, myMessage.getDiscoveryGroup());
    } else if (myMessage.equals(member)) { // group, ip & port are equal.
      logger.trace("skip message from myself", member);
    } else if (scb.addMember(member.getHostIp(), member.getClusterPort())) {
      logger.info("Broadcast message '{}' processed successfully", member);
    } else {
      logger.trace("Broadcast message '{}' already processed", member);
    }
  }

  @Override
  public void run() {
    // run in loop until doingShutdown is true...
    while (!doingShutdown) {
      try {
        byte[] buf = new byte[512];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        broadcastSocket.receive(packet);

        BroadcastMessage message = new BroadcastMessage(buf);

        logger.trace("Incoming broadcast: '{}' from {}:{}",
            message, packet.getAddress().getHostAddress(),packet.getPort());
        addMember(message);
      } catch (SocketException e) {
        if (doingShutdown) {
          logger.debug("doingShutdown and accept threw:" + e.getMessage());
        } else {
          logger.error("Error while listening", e);
        }
      } catch (IOException e) {
        // log it and continue in the loop...
        logger.error("IOException processing cluster message", e);
      }
    }
  }

  /**
   * Start broadcasting.
   */
  public void startListening() {
    logger.trace("... startListening()");
    this.broadcastListeningThread.setDaemon(true);
    this.broadcastListeningThread.start();
  }

  /**
   * Shutdown this listener.
   */
  public void shutdown() {
    doingShutdown = true;
    try {
      broadcastListeningThread.interrupt();
      broadcastSocket.close();
      broadcastListeningThread.join();
    } catch (InterruptedException ie) {
      // OK to ignore as expected to Interrupt for shutdown.
    }
  }
}
