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
 * todo - transparent long message support(but with like ever 2-3 packets an ack intermediate, for very long messages use the fragment stream behind the scenes)
 *
 * ConsiderTimeout = m * avRTT + a
 *
 * @author jokrey
 */
public interface P2LConversation {
    SocketAddress getPeer();
    int getAvRTT();
    int getHeaderSize();
    int getMaxPayloadSizePerPackage();

    void setMaxAttempts(int maxRetries);
    void setM(float m);
    void setA(int a);
    void setRM(int a); //retry multiplier


    P2LMessage initExpect(MessageEncoder encoded) throws IOException;
    P2LMessage answerExpect(MessageEncoder encoded) throws IOException;

    void initClose(MessageEncoder encoded) throws IOException; //STRICTLY requires a close on the other side.. answerClose can operate without(but it would always waste a packet, so ... don't)
    void answerClose(MessageEncoder encoded) throws IOException;

    //SPECIAL:: ONLY USE AS FIRST CONVO INSTRUCTION ON SERVER SIDE!!!
    P2LMessage initExpectClose(MessageEncoder encoded) throws IOException;
    //SPECIAL:: ONLY USE AS FIRST CONVO INSTRUCTION ON SERVER SIDE!!!
    void closeWith(MessageEncoder encoded) throws IOException;

    void close() throws IOException;




    //empty init shortcuts

    byte[] EMPTY_BYTES = new byte[0];
    default byte[] initExpect() throws IOException {
        return initExpectData(EMPTY_BYTES);
    }
    default byte[] initExpectClose() throws IOException {   //does not make sense for answer - if you answer with nothing you did something wrong..
        return initExpectDataClose(EMPTY_BYTES);
    }



    //special byte shortcuts

    default P2LMessage initExpect(byte[] bytes) throws IOException { return initExpect(MessageEncoder.from(getHeaderSize(), bytes)); }
    default P2LMessage answerExpect(byte[] bytes) throws IOException { return answerExpect(MessageEncoder.from(getHeaderSize(), bytes)); }
    default P2LMessage initExpectClose(byte[] bytes) throws IOException { return initExpectClose(MessageEncoder.from(getHeaderSize(), bytes)); }
    default void initClose(byte[] bytes) throws IOException { initClose(MessageEncoder.from(getHeaderSize(), bytes)); }
    default void answerClose(byte[] bytes) throws IOException { answerClose(MessageEncoder.from(getHeaderSize(), bytes)); }
    default void closeWith(byte[] bytes) throws IOException { closeWith(MessageEncoder.from(getHeaderSize(), bytes)); }
    default byte[] answerExpectData(MessageEncoder encoded) throws IOException { return answerExpect(encoded).asBytes(); }
    default byte[] initExpectData(MessageEncoder encoded) throws IOException { return initExpect(encoded).asBytes(); }
    default byte[] initExpectDataClose(MessageEncoder encoded) throws IOException { return initExpectClose(encoded).asBytes(); }
    default byte[] answerExpectData(byte[] bytes) throws IOException { return answerExpectData(MessageEncoder.from(getHeaderSize(), bytes)); }
    default byte[] initExpectData(byte[] bytes) throws IOException { return initExpectData(MessageEncoder.from(getHeaderSize(), bytes)); }
    default byte[] initExpectDataClose(byte[] bytes) throws IOException { return initExpectDataClose(MessageEncoder.from(getHeaderSize(), bytes)); }


    default MessageEncoder encode(Object... payloads) { return MessageEncoder.encodeAll(getHeaderSize(), payloads); }


    //todo - pause idea:
    //x, y communicating:
    //x sends data, y needs an unknown amount of time to calculate the response
    //  y: (a1, a2) = convo.initExpect("hallo")
    //  x: result = convo.answerPause(convo.encode(1, 2), timeout=20minutes)
    //  y: convo.pause()
    //  y: calculated = a1 + a2 //takes 10 minutes
    //  y: convo.answerClose(calculated)
    //  x: result was set to calculated
    //  x: convo.close()


    default P2LMessage answerExpectAfterPause(MessageEncoder encoded) {
        return answerExpectAfterPause(encoded,-1);
    }
    P2LMessage answerExpectAfterPause(MessageEncoder encoded, int timeout);
    void pause();
}