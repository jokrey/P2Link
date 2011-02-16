package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * Forced efficiency, short burst communication that uses the response as validation for receival.
 *    Tcp - in the optimal case - should work similarly and have similar performance...
 *
 * todo - stream support at intermediate points..
 *
 * @author jokrey
 */
public interface P2LConversation {
    SocketAddress getPeer();
    int getAvRTT();
    void setMaxAttempts(int maxRetries);
    void setM(float m);
    void setA(int a);

    P2LMessage initExpectMsg(byte[] bytes) throws IOException;
    P2LMessage answerExpectMsg(byte[] bytes) throws IOException;

    void initClose(byte[] bytes) throws IOException; //STRICTLY requires a close on the other side.. answerClose can operate without(but it would always waste a packet, so ... don't)
    void answerClose(byte[] bytes) throws IOException;
    void close() throws IOException;


    //SPECIAL:: ONLY USE AS FIRST CONVO RESULT ON SERVER SIDE!!!
    P2LMessage initExpectCloseMsg(byte[] bytes) throws IOException;
    void closeWith(byte[] bytes) throws IOException;


    default byte[] answerExpect(byte[] bytes) throws IOException {
        return answerExpectMsg(bytes).asBytes();
    }
    default byte[] initExpect(byte[] bytes) throws IOException {
        return initExpectMsg(bytes).asBytes();
    }
    default byte[] initExpectClose(byte[] bytes) throws IOException {
        return initExpectCloseMsg(bytes).asBytes();
    }

    byte[] EMPTY_BYTES = new byte[0];
    default byte[] initExpect() throws IOException {
        return initExpect(EMPTY_BYTES);
    }
    default byte[] initExpectClose() throws IOException {
        return initExpectClose(EMPTY_BYTES);
    }
    //does not make sense for answer - if you answer with nothing you did something wrong..
}
