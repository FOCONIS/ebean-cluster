package io.ebeaninternal.server.cluster;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.testng.Assert.*;

public class SocketConfigTest {

  @Test
  public void loadFromProperties() throws Exception {

    Properties properties = new Properties();
    properties.setProperty("ebean.cluster.bindAddr", "127.0.0.1 ");
    properties.setProperty("ebean.cluster.port", "9898 ");
    properties.setProperty("ebean.cluster.members", "127.0.0.1:9901,127.0.0.1:9902; 127.0.0.1:9903 ;  127.0.0.1:9904 ");
    properties.setProperty("ebean.cluster.threadPoolName", "somePoolName");

    ClusterBroadcastConfig config = new ClusterBroadcastConfig();
    config.loadFromProperties(properties);

    assertEquals(config.getBindAddr(), "127.0.0.1");
    assertEquals(config.getPort(), 9898);
    assertEquals(config.getThreadPoolName(), "somePoolName");
    assertThat(config.getMembers()).containsExactly("127.0.0.1:9901","127.0.0.1:9902","127.0.0.1:9903","127.0.0.1:9904");

  }

  @Test
  public void setters() {

    ClusterBroadcastConfig config = new ClusterBroadcastConfig();
    config.setThreadPoolName("somePoolName");
    config.setMembers(Arrays.asList("127.0.0.1:9901","127.0.0.1:9902","127.0.0.1:9903","127.0.0.1:9904"));
    config.setBindAddr("127.0.0.1");
    config.setPort(9898);

    assertEquals(config.getBindAddr(), "127.0.0.1");
    assertEquals(config.getPort(), 9898);
    assertEquals(config.getThreadPoolName(), "somePoolName");
    assertThat(config.getMembers()).containsExactly("127.0.0.1:9901","127.0.0.1:9902","127.0.0.1:9903","127.0.0.1:9904");

  }
}