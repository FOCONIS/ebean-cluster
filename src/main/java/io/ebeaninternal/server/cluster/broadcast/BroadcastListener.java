package io.ebeaninternal.server.cluster.broadcast;

import io.ebeaninternal.server.cluster.socket.SocketClusterAutoDiscoveryBroadcast;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Very simple broadcast listener
 *
 * A message is an ip with port e.g. 10.0.0.100:55500
 * If that host is unknown it will be added to the knownhosts
 */
public class BroadcastListener extends Thread {

    private String broadCastIp;
    private SocketClusterAutoDiscoveryBroadcast scb;
    private CopyOnWriteArraySet<String> members = new CopyOnWriteArraySet<>();

    private volatile boolean listening = false;

    public BroadcastListener(String broadCastIp, SocketClusterAutoDiscoveryBroadcast scb) {
        super("Ebean broadcast listener");

        this.broadCastIp = broadCastIp;
        this.scb = scb;
    }

    private void addMember(String member) {
        if (!members.contains(member)) {
            // register etc
            scb.addMember(member);

            members.add(member);
        }
    }

    public void stopListening() {
        listening = false;
    }

    @Override
    public void run() {
        listening = true;

        try(MulticastSocket socket = new MulticastSocket(4446)) {
            InetAddress group = InetAddress.getByName(broadCastIp);
            socket.joinGroup(group);

            DatagramPacket packet;

            while(listening) {
                byte[] buf = new byte[256];
                packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                String received = new String(packet.getData());
                System.out.println("Received: " + received);

                addMember(received);
            }

            socket.leaveGroup(group);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        } finally {
            listening = false;
        }
    }
}
