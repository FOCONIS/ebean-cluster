package io.ebeaninternal.server.cluster.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The message broadcast around the cluster.
 */
public class ClusterMessage {

  private static final int TYPE_TRANSACTION = 0;
  private static final int TYPE_REGISTER = 1;
  private static final int TYPE_UNREGISTER = 2;
  private static final int TYPE_PING = 3;
  private static final int TYPE_PONG = 4;

  private final int eventType;

  private final String registerHost;

  private final byte[] data;

  private final long timestamp;

  /**
   * Create a register message.
   */
  public static ClusterMessage register(String registerHost, boolean register) {
    return new ClusterMessage(registerHost, register);
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
  public static ClusterMessage ping() {
    return new ClusterMessage(TYPE_PING, System.currentTimeMillis());
  }

  /**
   * Create for register online/offline message.
   */
  private ClusterMessage(String registerHost, boolean register) {
    this.registerHost = registerHost;
    this.eventType = register ? TYPE_REGISTER : TYPE_UNREGISTER;
    this.data = null;
    this.timestamp = 0;
  }

  /**
   * Create for a transaction message.
   */
  private ClusterMessage(byte[] data) {
    this.eventType = TYPE_TRANSACTION;
    this.data = data;
    this.registerHost = null;
    this.timestamp = 0;
  }

  /**
   * Create for ping & pong.
   */
  private ClusterMessage(int type, long timestamp) {
    this.eventType = type;
    this.data = null;
    this.registerHost = null;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (registerHost != null) {
      sb.append("type ");
      sb.append(eventType);
      sb.append(" ");
      sb.append(registerHost);
    } else {
      sb.append("transEvent ");
    }
    return sb.toString();
  }

  /**
   * Return true if this is a register event as opposed to a transaction message.
   */
  public boolean isRegisterEvent() {
    return eventType == TYPE_REGISTER || eventType == TYPE_UNREGISTER;
  }

  /**
   * Return the register host for online/offline message.
   */
  public String getRegisterHost() {
    return registerHost;
  }

  /**
   * Return true if register is true for a online/offline message.
   */
  public boolean isRegister() {
    return eventType == TYPE_REGISTER;
  }

  /**
   * Return true if register is true for a online/offline message.
   */
  public boolean isPing() {
    return eventType == TYPE_REGISTER;
  }

  /**
   * Return true if register is true for a online/offline message.
   */
  public boolean isPong() {
    return eventType == TYPE_REGISTER;
  }

  /**
   * Return the raw message data.
   */
  public byte[] getData() {
    return data;
  }

  /**
   * Write the message in binary form.
   */
  public void write(DataOutputStream dataOutput) throws IOException {

    dataOutput.writeInt(eventType);
    if (eventType == TYPE_TRANSACTION) {
      dataOutput.writeInt(data.length);
      dataOutput.write(data);
    } else if (isRegisterEvent()){
      dataOutput.writeUTF(getRegisterHost());
    } else if (eventType == TYPE_PING || eventType == TYPE_PONG) {
      dataOutput.writeLong(timestamp);
    }
    dataOutput.flush();
  }

  /**
   * Read the message from binary form.
   */
  public static ClusterMessage read(DataInputStream dataInput) throws IOException {
    int type = dataInput.read();
    switch(type) {
      case TYPE_TRANSACTION:
        int length = dataInput.readInt();
        byte[] data = new byte[length];
        dataInput.readFully(data);
        return new ClusterMessage(data);
      case TYPE_REGISTER:
      case TYPE_UNREGISTER:
        String host = dataInput.readUTF();
        return new ClusterMessage(host, type == TYPE_REGISTER);
      case TYPE_PING:
      case TYPE_PONG:
        return new ClusterMessage(type, dataInput.readLong());
      default:
        throw new UnsupportedOperationException("Unknown type: " + type);
    }
  }

  public ClusterMessage getPong() {
    return new ClusterMessage(TYPE_PONG, timestamp);
  }

  public long getTimestamp() {
    return timestamp;
  }

}
