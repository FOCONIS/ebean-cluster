package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.SocketConfig;
import io.ebeaninternal.server.cluster.broadcast.BroadcastListener;
import io.ebeaninternal.server.cluster.broadcast.Broadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Broadcast messages across the cluster using sockets.
 */
public class SocketClusterAutoDiscoveryBroadcast extends SocketClusterBroadcast {

    private static final Logger clusterLogger = LoggerFactory.getLogger("org.avaje.ebean.Cluster");

    private static final Logger logger = LoggerFactory.getLogger(SocketClusterAutoDiscoveryBroadcast.class);

    private String localHostname;
    private int port;
    private BroadcastListener listener;
    private Broadcaster broadcaster;

    public SocketClusterAutoDiscoveryBroadcast(
            ClusterManager manager,
            SocketConfig config
    ) {
        super(manager, config);

        this.port = config.getClusterPort();

        Enumeration e;
        try {
            e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface n = (NetworkInterface) e.nextElement();
                Enumeration ee = n.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress i = (InetAddress) ee.nextElement();

                    if (i.getHostAddress().startsWith(config.getSubDomain())) {
                        localHostname = i.getHostAddress();
                        break;
                    }
                }
            }
        } catch (SocketException e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public void startup() {
        super.startup();

        if (localHostname != null) {
            listener = new BroadcastListener("224.0.0.180", this);
            broadcaster = new Broadcaster("224.0.0.180", localHostname);

            listener.start();
            broadcaster.start();
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();

        if (broadcaster != null) {
            broadcaster.stopBroadcasting();
            broadcaster.interrupt();

            broadcaster = null;
        }

        if (listener != null) {
            listener.stopListening();
            listener.interrupt();

            listener = null;
        }
    }

    public void addMember(String member) {
        if (localHostname != null && !localHostname.equals(member)) {
            SocketClient client = new SocketClient(new InetSocketAddress(localHostname, port));

            addMember(client);
        }
    }
}
