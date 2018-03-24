package io.ebeaninternal.server.cluster.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The message broadcast around the cluster.
 */
public class ClusterMessage {
  public enum Type {
    TRANSACTION, REGISTER, DEREGISTER, PING, PONG
  }

  private final Type type;

  private final int port;

  private final byte[] data;

  private final long timestamp;

  /**
   * Create a register message.
   */
  public static ClusterMessage register(int port) {
    return new ClusterMessage(Type.REGISTER, port);
  }
  /**
   * Create a deregister message.
   */
  public static ClusterMessage deregister(int port) {
    return new ClusterMessage(Type.DEREGISTER, port);
  }

  /**
   * Create a transaction message.
   */
  public static ClusterMessage transEvent(byte[] data) {
    return new ClusterMessage(data);
  }

  /**
   * Create a ping message.
   */
  public static ClusterMessage ping(int port) {
    return new ClusterMessage(Type.PING, port, System.currentTimeMillis());
  }

  /**
   * Create for register online/offline message.
   */
  private ClusterMessage(Type eventType, int port) {
    this.type = eventType;
    this.data = null;
    this.timestamp = 0;
    this.port = port;
  }

  /**
   * Create for a transaction message.
   */
  private ClusterMessage(byte[] data) {
    this.type = Type.TRANSACTION;
    this.data = data;
    this.port = 0;
    this.timestamp = 0;
  }

  /**
   * Create for ping & pong.
   */
  private ClusterMessage(Type type, int port, long timestamp) {
    this.type = type;
    this.data = null;
    this.port = port;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    switch (type) {
    case TRANSACTION:
      return "transEvent, payload: " + data.length + " bytes";
    case REGISTER:
      return "register";
    case DEREGISTER:
      return "deregister";
    case PING:
      return "ping";
    case PONG:
      return "pong";
    default:
      return "txpe " + type;
    }
  }

  public Type getType() {
    return type;
  }

  public int getPort() {
    return port;
  }

  public byte[] getData() {
    return data;
  }

  long getRTT() {
    return System.currentTimeMillis() - timestamp;
  }

  /**
   * Write the message in binary form.
   */
  public void write(DataOutputStream dataOutput) throws IOException {

    dataOutput.writeInt(type.ordinal());
    if (type == Type.TRANSACTION) {
      dataOutput.writeInt(data.length);
      dataOutput.write(data);
    } else {
      dataOutput.writeInt(port);
      if (type == Type.PING || type == Type.PONG) {
        dataOutput.writeLong(timestamp);
      }
    }
    dataOutput.flush();
  }

  /**
   * Read the message from binary form.
   */
  public static ClusterMessage read(DataInputStream dataInput) throws IOException {
    Type type = Type.values()[dataInput.readInt()];
    switch(type) {
      case TRANSACTION:
        int length = dataInput.readInt();
        byte[] data = new byte[length];
        dataInput.readFully(data);
        return new ClusterMessage(data);
      case REGISTER:
      case DEREGISTER:
        return new ClusterMessage(type, dataInput.readInt());
      case PING:
      case PONG:
        return new ClusterMessage(type, dataInput.readInt(), dataInput.readLong());
      default:
        throw new UnsupportedOperationException("Unknown type: " + type);
    }
  }

  public ClusterMessage pong(int port) {
    return new ClusterMessage(Type.PONG, port, timestamp);
  }

  public long getTimestamp() {
    return timestamp;
  }

}
