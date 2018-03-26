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
    return new ClusterMessage(Type.PING, port);
  }

  /**
   * Create a pong message.
   */
  public static ClusterMessage pong(int port) {
    return new ClusterMessage(Type.PONG, port);
  }

  /**
   * Create for control message.
   */
  private ClusterMessage(Type eventType, int port) {
    this.type = eventType;
    this.data = null;
    this.port = port;
  }

  /**
   * Create for a transaction message.
   */
  private ClusterMessage(byte[] data) {
    this.type = Type.TRANSACTION;
    this.data = data;
    this.port = 0;
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
      case PING:
      case PONG:
        return new ClusterMessage(type, dataInput.readInt());
      default:
        throw new UnsupportedOperationException("Unknown type: " + type);
    }
  }

}
