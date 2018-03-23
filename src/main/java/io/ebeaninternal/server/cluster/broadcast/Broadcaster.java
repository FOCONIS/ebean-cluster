package io.ebeaninternal.server.cluster.broadcast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sends broadcast messages periodically to broadcast IP.
 * User: rnentjes
 * Date: 20-3-18
 * Time: 12:58
 */
public class Broadcaster implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

  /**
   * The broadcasting thread.
   */
  private final Thread broadcasterThread;

  /**
   * The multicast socket.
   */
  private final MulticastSocket broadcastSocket;

  /**
   * Packet, we send every broadcastInterval.
   */
  private final DatagramPacket packet;

  /**
   * The broadcast message (for logging).
   */
  private final BroadcastMessage message;

  /**
   * The interval in milliseconds.
   */
  private final int broadcastInterval;


  /**
   * shutting down flag.
   */
  private boolean doingShutdown;


  /**
   * Create a new broadcaster.
   *
   * @param address
   *    the broadcast address (should start with 224.0.0. and port &gt; 1024 is a good idea)
   * @param broadcastInterval
   *    the intervall in milliseconds
   * @param message
   *    the message
   *
   * @throws IOException
   */
  public Broadcaster(InetSocketAddress address, int broadcastInterval, BroadcastMessage message) throws IOException {
    byte[] buf = message.getBytes();
    packet = new DatagramPacket(buf, buf.length, address);

    this.message = message;
    this.broadcasterThread = new Thread(this, "EbeanClusterBroadcaster");
    this.broadcastSocket = new MulticastSocket(address.getPort());
    this.broadcastSocket.setSoTimeout(60000);
    this.broadcastInterval = broadcastInterval;
  }

  @Override
  public void run() {
    // run in loop until doingShutdown is true...
    while (!doingShutdown) {
      try {
        broadcastSocket.send(packet);
        logger.trace("Sending broadcast '{}' to {}:{}", message, packet.getAddress().getHostAddress(), packet.getPort());
        Thread.sleep(broadcastInterval);
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
  public void startBroadcasting() {
    logger.trace("... startBroadcasting()");
    this.broadcasterThread.setDaemon(true);
    this.broadcasterThread.start();
  }

  /**
   * Shutdown this listener.
   */
  public void shutdown() {
    doingShutdown = true;
    try {
      broadcasterThread.interrupt();
      broadcastSocket.close();
      broadcasterThread.join();
    } catch (InterruptedException ie) {
      // OK to ignore as expected to Interrupt for shutdown.
    }
  }
}
