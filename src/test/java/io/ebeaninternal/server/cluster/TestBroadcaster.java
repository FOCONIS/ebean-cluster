/*
 * Licensed Materials - Property of FOCONIS AG
 * (C) Copyright FOCONIS AG.
 */

package io.ebeaninternal.server.cluster;

import java.util.Properties;

import org.testng.annotations.Test;

import io.ebean.EbeanServer;
import io.ebean.config.ContainerConfig;
import io.ebean.testdouble.TDSpiEbeanServer;
import io.ebeaninternal.server.transaction.RemoteTransactionEvent;

/**
 * TODO.
 *
 * @author Roland Praml, FOCONIS AG
 *
 */
public class TestBroadcaster {

  private class TestServer extends TDSpiEbeanServer {

    RemoteTransactionEvent event;

    TestServer(String name) {
      super(name);
    }

    @Override
    public void remoteTransactionEvent(RemoteTransactionEvent event) {
      this.event = event;
    }
  }

  /**
   * @param args
   * @throws InterruptedException
   */
  @Test
  public void test() throws InterruptedException {
    ContainerConfig containerConfig = new ContainerConfig();
    Properties properties = new Properties();
    properties.put("ebean.cluster.mode", "autodiscovery");
    //properties.put("ebean.cluster.discovery.interval", "5000");
    //properties.put("ebean.cluster.discovery.net", "10.75.1.0/24");
    containerConfig.setProperties(properties);
    ClusterManager manager = new ClusterManager(containerConfig );


    EbeanServer server = new TestServer("test");

    manager.registerServer(server);
    for (int i = 0 ; i <  100 ; i++) {
      Thread.sleep((int)(Math.random()*100000));
      RemoteTransactionEvent event = new RemoteTransactionEvent("test");
      event.cacheClearAll();
      manager.broadcast(event);
    }
    manager.shutdown();

  }

}
