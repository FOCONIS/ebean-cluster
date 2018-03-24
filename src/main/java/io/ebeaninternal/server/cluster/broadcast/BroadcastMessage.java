package io.ebeaninternal.server.cluster.broadcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.util.UUID;

/**
 * A broadcast message is sent periodically
 *
 * it contains hostGroup, host UUID and hostPort of the member.
 * Only members of the same hostGroup will be added.
 * UUID is required to check if this is our own packet.
 * @author Roland Praml, FOCONIS AG
 *
 */
public class BroadcastMessage implements Serializable {

  private static final long serialVersionUID = 2684684366359766679L;

  private final String discoveryGroup;
  private final UUID hostUuid;
  private final int clusterPort;


  public BroadcastMessage(String discoveryGroup, UUID hostUuid, int hostPort) {
    this.discoveryGroup = discoveryGroup;
    this.hostUuid = hostUuid;
    this.clusterPort = hostPort;
  }

  /**
   * Constructor for incomming message from socket
   */
  public BroadcastMessage(byte[] rawMessage) throws IOException {
    try ( DataInputStream in = new DataInputStream(new ByteArrayInputStream(rawMessage))) {
      if (in.readLong() != serialVersionUID) {
        throw new StreamCorruptedException("magic number does not match");
      }
      discoveryGroup = in.readUTF();
      hostUuid = new UUID(in.readLong(), in.readLong());
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
      out.writeLong(hostUuid.getMostSignificantBits());
      out.writeLong(hostUuid.getLeastSignificantBits());
      out.writeInt(clusterPort);
    } catch (IOException e) {}

    return baos.toByteArray();
  }

  public String getDiscoveryGroup() {
    return discoveryGroup;
  }

  public int getClusterPort() {
    return clusterPort;
  }

  public UUID getHostUuid() {
    return hostUuid;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(discoveryGroup).append('@');
    sb.append(hostUuid).append(':').append(clusterPort);
    return sb.toString();
  }
}
