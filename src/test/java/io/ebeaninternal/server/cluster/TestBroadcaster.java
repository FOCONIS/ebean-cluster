/*
 * Licensed Materials - Property of FOCONIS AG
 * (C) Copyright FOCONIS AG.
 */

package io.ebeaninternal.server.cluster;

import java.util.Properties;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import io.ebean.EbeanServer;
import io.ebean.config.ContainerConfig;

/**
 * TODO.
 *
 * @author Roland Praml, FOCONIS AG
 *
 */
public class TestBroadcaster {

  /**
   * @param args
   * @throws InterruptedException
   */
  @Test
  public void test() throws InterruptedException {
    ContainerConfig containerConfig = new ContainerConfig();
    Properties properties = new Properties();
    properties.put("ebean.cluster.discovery.enabled", "true");
    //properties.put("ebean.cluster.discovery.net", "10.75.1.0/24");
    containerConfig.setProperties(properties);
    ClusterManager manager = new ClusterManager(containerConfig );


    EbeanServer server = Mockito.mock(EbeanServer.class);
    Mockito.when(server.getName()).thenReturn("test");

    manager.registerServer(server );

    Thread.sleep(1000000);
    manager.shutdown();

  }

}
