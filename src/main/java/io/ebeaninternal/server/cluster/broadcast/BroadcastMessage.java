package io.ebeaninternal.server.cluster.broadcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.Objects;

/**
 * A broadcast message is sent periodically to a given broadcast IP.
 *
 * it contains hostGroup, hostName and hostPort of the member.
 * Only members of the same hostGroup will be added.
 *
 * @author Roland Praml, FOCONIS AG
 *
 */
public class BroadcastMessage implements Serializable {

  private static final long serialVersionUID = 8861907251314300963L;

  private final String discoveryGroup;
  private final String hostIp;
  private final int clusterPort;

  public BroadcastMessage(String discoveryGroup, String hostName, int hostPort) {
    this.discoveryGroup = discoveryGroup;
    this.hostIp = hostName;
    this.clusterPort = hostPort;
  }

  /**
   * Constructor for incomming message from socket
   */
  public BroadcastMessage(byte[] rawMessage) throws IOException {
    try ( DataInputStream in = new DataInputStream(new ByteArrayInputStream(rawMessage))) {
      if (in.readLong() != serialVersionUID) {
        throw new InvalidObjectException("magic number does not match");
      }
      discoveryGroup = in.readUTF();
      hostIp = in.readUTF();
      clusterPort = in.readInt();
    }
  }

  /**
   * Returns the bytes that are ready to be broadcasted.
   */
  public byte[] getBytes() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (  DataOutputStream out = new DataOutputStream(baos)) {
      out.writeLong(serialVersionUID);
      out.writeUTF(discoveryGroup);
      out.writeUTF(hostIp);
      out.writeInt(clusterPort);
    } catch (IOException e) {}

    return baos.toByteArray();
  }

  public String getDiscoveryGroup() {
    return discoveryGroup;
  }

  public String getHostIp() {
    return hostIp;
  }

  public int getClusterPort() {
    return clusterPort;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(discoveryGroup).append('@');
    sb.append(hostIp).append(':').append(clusterPort);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(discoveryGroup, hostIp) + clusterPort;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof BroadcastMessage) {
      BroadcastMessage other = (BroadcastMessage)obj;
      return other.clusterPort == clusterPort
          && Objects.equals(other.discoveryGroup, discoveryGroup)
          && Objects.equals(other.hostIp, hostIp);
    } else  {
      return false;
    }
  }

}
