package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.lib.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Serverside multithreaded socket listener. Accepts connections and dispatches
 * them to an appropriate handler.
 * <p>
 * This is designed as a single port listener, where part of the connection
 * protocol determines which service the client is requesting (rather than a
 * port per service).
 * </p>
 * <p>
 * It has its own daemon background thread that handles the accept() loop on the
 * ServerSocket.
 * </p>
 */
class SocketClusterListener implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(SocketClusterListener.class);

  /**
   * The server socket used to listen for requests.
   */
  private final ServerSocket serverListenSocket;

  /**
   * The listening thread.
   */
  private final Thread listenerThread;

  /**
   * The pool of threads that actually do the parsing execution of requests.
   */
  private final ExecutorService service;

  private final SocketClusterBroadcast owner;

  /**
   * shutting down flag.
   */
  boolean doingShutdown;

  /**
   * Whether the listening thread is busy assigning a request to a thread.
   */
  boolean isActive;

  /**
   * Construct with a given thread pool name.
   */
  public SocketClusterListener(SocketClusterBroadcast owner, SocketAddress bindAddr, String poolName) {
    this.owner = owner;
    this.service = Executors.newCachedThreadPool(new DaemonThreadFactory(poolName));
    try {
      this.serverListenSocket = new ServerSocket();
      this.serverListenSocket.bind(bindAddr);
      this.serverListenSocket.setSoTimeout(60000);
      this.listenerThread = new Thread(this, "EbeanClusterListener");

    } catch (IOException e) {
      String msg = "Error starting cluster socket listener on " + bindAddr;
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Start listening for requests.
   */
  public void startListening() {
    logger.trace("... startListening()");
    this.listenerThread.setDaemon(true);
    this.listenerThread.start();
  }

  /**
   * Shutdown this listener.
   */
  public void shutdown() {
    doingShutdown = true;
    try {
      if (isActive) {
        synchronized (listenerThread) {
          try {
            listenerThread.wait(1000);
          } catch (InterruptedException e) {
            // OK to ignore as expected to Interrupt for shutdown.
          }
        }
      }
      listenerThread.interrupt();
      serverListenSocket.close();
    } catch (IOException e) {
      logger.error("Error shutting down listener", e);
    }

    service.shutdown();
  }

  /**
   * This is a runnable and so this must be public. Don't call this externally
   * but rather call the startListening() method.
   */
  @Override
  public void run() {
    // run in loop until doingShutdown is true...
    while (!doingShutdown) {
      try {
        synchronized (listenerThread) {
          Socket clientSocket = serverListenSocket.accept();
          isActive = true;

          Runnable request = new RequestProcessor(owner, clientSocket);
          service.execute(request);

          isActive = false;
        }
      } catch (SocketException e) {
        if (doingShutdown) {
          logger.debug("doingShutdown and accept threw:" + e.getMessage());
        } else {
          logger.error("Error while listening", e);
        }
      } catch (InterruptedIOException e) {
        // this will happen when the server is very quiet.
        // that is, no requests
        logger.debug("Possibly expected due to accept timeout? {}", e.getMessage());

      } catch (IOException e) {
        // log it and continue in the loop...
        logger.error("IOException processing cluster message", e);
      }
    }
  }

}
