package io.ebeaninternal.server.cluster.broadcast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.TimeUnit;

/**
 * User: rnentjes
 * Date: 20-3-18
 * Time: 12:58
 */
public class Broadcaster extends Thread {

    private String broadCastIp;
    private String hostname;
    private volatile boolean broadcasting = false;

    public Broadcaster(String broadCastIp, String hostname) {
        this.broadCastIp = broadCastIp;
        this.hostname = hostname;
    }

    public void stopBroadcasting() {
        broadcasting = false;
    }

    @Override
    public void run() {
        broadcasting = true;

        try(MulticastSocket socket = new MulticastSocket(4446)) {
            byte[] buf = hostname.getBytes();
            InetAddress group = InetAddress.getByName(broadCastIp);

            while(broadcasting) {
                DatagramPacket packet;
                packet = new DatagramPacket(buf, buf.length, group, 4446);

                socket.send(packet);

                try {
                    sleep(TimeUnit.MINUTES.toMillis(1));
                } catch (InterruptedException e) {
                    // expected
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            broadcasting = false;
        }
    }
}
