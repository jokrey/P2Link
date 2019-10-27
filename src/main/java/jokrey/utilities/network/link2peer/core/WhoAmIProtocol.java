package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.R_WHO_AM_I_ANSWER;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.SL_WHO_AM_I;

/**
 * @author jokrey
 */
public class WhoAmIProtocol {
    public static void asReceiver(P2LNodeInternal parent, DatagramPacket receivedPacket) throws IOException {
        parent.send(P2LMessage.createSendMessage(R_WHO_AM_I_ANSWER, receivedPacket.getAddress().getCanonicalHostName()+":"+receivedPacket.getPort()), receivedPacket.getSocketAddress());
    }

    public static String whoAmI(P2LNodeInternal parent, P2Link link, SocketAddress outgoing) throws IOException {
        parent.send(P2LMessage.createSendMessageFrom(SL_WHO_AM_I), outgoing);
        return parent.futureForInternal(link, R_WHO_AM_I_ANSWER).get(2500).asString();
    }
}
