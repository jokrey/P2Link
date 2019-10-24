package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.mcnp.MCNP_Connection;
import jokrey.utilities.network.mcnp.io.MCNP_ClientIO;
import jokrey.utilities.network.mcnp.io.MCNP_ConnectionIO;

import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

/**
 * @author jokrey
 */
class PeerConnection implements AutoCloseable {
    final P2Link peerLink;
    private final MCNP_Connection connection;
    private final P2LNodeInternal node;
//    private long lastSuccessfulConversation = -1;  //maybe used for a ping like system later

    PeerConnection(P2LNodeInternal node, MCNP_Connection connection, P2Link peerLink) {
        this.peerLink = peerLink;
        this.connection = connection;
        this.node = node;
    }
    PeerConnection(P2LNodeInternal node, P2Link peerLink, Socket connection) throws IOException {
        this(node, new MCNP_ConnectionIO(connection), peerLink);
    }

    @Override public void close() throws IOException {
        connection.close();
    }

    void tryClose() {
        connection.tryClose();
    }

    MCNP_Connection raw() {
        return connection;
    }


    synchronized void sendSuperCause(int superCause) throws IOException {
        raw().send_cause(superCause);
    }
    synchronized void send(int msgType, byte[] message) throws IOException {
        try {
            raw().send_cause(msgType);
            raw().send_variable(message);
        } catch (IOException e) {
            node.markBrokenConnection(peerLink);
            throw e;
        }
    }
    synchronized boolean trySend(int msgType, byte[] message) {
        try {
            raw().send_cause(msgType);
            raw().send_variable(message);
            return true;
        } catch (IOException e) {
            node.markBrokenConnection(peerLink);
            return false;
        }
    }

    P2LFuture<P2LMessage> futureRead(int msgType) {
        return node.futureForInternal(peerLink, msgType);
    }


    //as server
    static class Incoming extends PeerConnection {
        Incoming(P2LNodeInternal node, P2Link validLink, MCNP_Connection connection) {
            super(node, connection, validLink);
        }
    }

    //as client
    static class Outgoing extends PeerConnection {
        Outgoing(P2LNodeInternal node, P2Link link) throws IOException {
            super(node, link, MCNP_ClientIO.getConnectionToServer(link.ipOrDns, link.port, 2000));
        }
    }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeerConnection that = (PeerConnection) o;
        return Objects.equals(peerLink, that.peerLink) && Objects.equals(connection, that.connection);
    }
    @Override public int hashCode() { return Objects.hash(peerLink, connection); }
    @Override public String toString() {
        return "PeerConnection{peerLink=" + peerLink + ", connection=" + connection + '}';
    }

//    interface Conversation {
//        void converse(MCNP_Connection rawConnection) throws IOException;
//    }
//    interface ConversationForResult<R> {
//        R converse(MCNP_Connection rawConnection) throws IOException;
//    }
}
