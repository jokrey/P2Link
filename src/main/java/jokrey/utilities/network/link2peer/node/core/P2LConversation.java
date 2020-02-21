package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * Forced efficiency, short burst communication that uses the response as validation for receival.
 *    Tcp - in the optimal case - should work similarly and have similar performance...
 *
 * todo - stream support at intermediate points..
 *
 * ConsiderTimeout = m * avRTT + a
 *
 * @author jokrey
 */
public interface P2LConversation {
    SocketAddress getPeer();
    int getAvRTT();
    void setMaxAttempts(int maxRetries);
    void setM(float m);
    void setA(int a);

    int headerSize();

    P2LMessage initExpectMsg(MessageEncoder encoded) throws IOException;
    P2LMessage answerExpectMsg(MessageEncoder encoded) throws IOException;

    void initClose(MessageEncoder encoded) throws IOException; //STRICTLY requires a close on the other side.. answerClose can operate without(but it would always waste a packet, so ... don't)
    void answerClose(MessageEncoder encoded) throws IOException;

    //SPECIAL:: ONLY USE AS FIRST CONVO RESULT ON SERVER SIDE!!!
    P2LMessage initExpectCloseMsg(MessageEncoder encoded) throws IOException;
    void closeWith(MessageEncoder encoded) throws IOException;

    void close() throws IOException;

    default byte[] answerExpect(MessageEncoder encoded) throws IOException {
        return answerExpectMsg(encoded).asBytes();
    }
    default byte[] initExpect(MessageEncoder encoded) throws IOException {
        return initExpectMsg(encoded).asBytes();
    }
    default byte[] initExpectClose(MessageEncoder encoded) throws IOException {
        return initExpectCloseMsg(encoded).asBytes();
    }

    byte[] EMPTY_BYTES = new byte[0];
    default byte[] initExpect() throws IOException {
        return initExpect(EMPTY_BYTES);
    }
    default byte[] initExpectClose() throws IOException {   //does not make sense for answer - if you answer with nothing you did something wrong..
        return initExpectClose(EMPTY_BYTES);
    }


    default P2LMessage initExpectMsg(byte[] bytes) throws IOException { return initExpectMsg(MessageEncoder.from(headerSize(), bytes)); }
    default P2LMessage answerExpectMsg(byte[] bytes) throws IOException { return answerExpectMsg(MessageEncoder.from(headerSize(), bytes)); }
    default void initClose(byte[] bytes) throws IOException { initClose(MessageEncoder.from(headerSize(), bytes)); }
    default void answerClose(byte[] bytes) throws IOException { answerClose(MessageEncoder.from(headerSize(), bytes)); }
    default P2LMessage initExpectCloseMsg(byte[] bytes) throws IOException { return initExpectCloseMsg(MessageEncoder.from(headerSize(), bytes)); }
    default void closeWith(byte[] bytes) throws IOException { closeWith(MessageEncoder.from(headerSize(), bytes)); }
    default byte[] answerExpect(byte[] bytes) throws IOException { return answerExpect(MessageEncoder.from(headerSize(), bytes)); }
    default byte[] initExpect(byte[] bytes) throws IOException { return initExpect(MessageEncoder.from(headerSize(), bytes)); }
    default byte[] initExpectClose(byte[] bytes) throws IOException { return initExpectClose(MessageEncoder.from(headerSize(), bytes)); }


    default MessageEncoder encode(Object... payloads) { return MessageEncoder.encodeAll(headerSize(), payloads); }
}