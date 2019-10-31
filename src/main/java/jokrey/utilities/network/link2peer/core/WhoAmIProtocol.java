package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.R_WHO_AM_I_ANSWER;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.SL_WHO_AM_I;

/**
 * @author jokrey
 */
public class WhoAmIProtocol {
    public static String asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        return parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
                parent.expectInternalMessage(to, R_WHO_AM_I_ANSWER)
                .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessageFrom(SL_WHO_AM_I), to))
        ).asString();
    }
    static void asAnswerer(P2LNodeInternal parent, DatagramPacket receivedPacket) throws IOException {
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_WHO_AM_I_ANSWER, P2LMessage.EXPIRE_INSTANTLY, toString(receivedPacket.getSocketAddress())), receivedPacket.getSocketAddress());
    }


    public static String toString(SocketAddress from) {
        InetSocketAddress f = (InetSocketAddress)from;
        return f.getAddress().getCanonicalHostName()+":"+f.getPort();
    }
    public static InetSocketAddress fromString(String str) {
        if(str == null) return null;
        String[] split = str.split(":");
        return new InetSocketAddress(split[0], Integer.parseInt(split[1]));
    }
}
