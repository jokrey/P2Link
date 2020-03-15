package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * Forced efficiency, short burst communication that uses the response as validation for receival.
 *    Tcp - in the optimal case - should work similarly and have similar performance...
 *
 * todo - transparent long message support(but with like ever 2-3 packets an ack intermediate, for very long messages use the fragment stream behind the scenes)
 * todo - out-of-the-box/appealing support for error codes and branching based on those codes
 * todo(!) - asynchronous version in which instead of returning P2LMessages the methods return P2LFuture<P2LMessage>.
 *         - Here two things are important: Maintaining performance and loosing the need to keep constantly keep the thread that handles the conversation (a (greenish) thread is needed to handle incoming messages anyways).
 *         - Additionally very cool: associated branching based on first-byte-codes in the message.
 *
 * Timeout occurs after = maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM
 *
 * Idea(s):   - see ConversationTest class for further, running examples - note that a answerExpect or
 *   1 (ping pong, server closes)
 *      client: val m1 = convo_client.initExpect(m0)
 *      server: init_thread(m0)
 *      server: val m2 = convo_server.answerExpect(m1)
 *      client: convo_client.answerClose(m2)
 *      server: convo_server.close()
 *   2 (ping pong, client closes)
 *      client: val m1 = convo_client.initExpect(m0)
 *      server: init_thread(m0)
 *      server: convo_server.answerClose(m1)
 *      client: convo_client.close()
 *   3 (server pauses conversation [client pausing works analogous])
 *      client: val m1 = convo_client.initExpectAfterPause(m0, timeout=10000)
 *      server: init_thread(m0)
 *      server: convo_server.pause()
 *      server: calculating/io operation/remote query
 *      server: convo_client.answerClose(m1)
 *      client: convo_client.close()
 *   4 (general structure)
 *      client: val m1 = initExpect(m0)
 *      server: init_thread(m0)
 *      server+client: answerExpect ping pong between client and server | answerExpectAfterPause ping pong
 *      server/client: answerClose(mx)
 *      client/server: close()
 *
 * FALSELY RESENT M0(init) package.
 *    The 'previous' functionality does catch a few of the falsely 'resent' issues,
 *       but not if a package is seriously delayed, then it is quite possible that a conversation is restarted
 *       this also naturally occurs when using 'initClose' or 'initExpectClose'.
 *    If it is required that server code is only executed once for a given operation, then implement a short handshake protocol up front
 *       if the conversation is falsely restarted by a resent message on client side - then the server will compute and answer the with the first part of the handshake(m1)
 *       however on client side no-one is listening and the package is discarded - the server will attempt to resend 'maxAttempts' times, which is sad, but at least not recalculate anything important.
 *
 *
 * NOT THREAD SAFE IN IT'S METHODS. i.e. CANNOT be used from different threads without further synchronization. Such further synchronization is out of the box for the async methods which can possibly operate on different threads.
 *
 * @author jokrey
 */
public interface P2LConversation {
    /** @return the socket address of the peer this conversation attempts to communicate with. */
    SocketAddress getPeer();
    /** @return current average round trip time of all packages with the peer. */
    int getAvRTT();
    /** @return header size of all packages sent and received by this conversation, relevant for message encoder offset calculations. */
    int getHeaderSize();
    /** @return Max number of bytes that can be sent in a single package within this conversation(with this specific peer). */
    int getMaxPayloadSizePerPackage();

    /** Set the maximum number of attempts this conversation will make to send every single package(has to be greater than or equal to 1). */
    void setMaxAttempts(int maxRetries);
    /** Set the m(ultiplier) of the timeout function 'maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM' */
    void setM(float m);
    /** Set the a(summand) of the timeout function 'maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM' */
    void setA(int a);
    /** Set the rM(retryMultiplier) of the timeout function 'maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM' */
    void setRM(int a); //retry multiplier


    /** Encodes the given payloads into a new message encoder object with the correct offset for it to be directly passed into this conversations methods */
    default MessageEncoder encode(Object... payloads) { return MessageEncoder.encodeAll(getHeaderSize(), payloads); }
    /** {@link #encoder(int)}, with initial capacity set to 64 bytes */
    default MessageEncoder encoder() {return encoder( 64);}
    /**
     * Creates a new message encoder with the correct offset for it to be directly passed into this conversations methods with arbitrary encoded data.
     * @param initial_capacity the initial capacity of the byte array backing the encoded data
     */
    default MessageEncoder encoder(int initial_capacity) {return new MessageEncoder(getHeaderSize(), initial_capacity);}



    /**
     * On server or client after a previous answerClose on the other side.
     * This operation can be the last on either convo side. No further calls to the conversation object shall be done after calling this method.
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    void close() throws IOException;
    /**
     * Attempts to call {@link #close()} if that fails, because an exception is thrown nothing is done.
     * This is useful in async call cascades.
     * @return whether close was successful.
     */
    default boolean tryClose() {
        try {
            close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }



    /**
     * Only as first instruction to the convo on client(convo opening) side.
     * Sends the first message to the server and starts the response thread there.
     * Strictly requires a answerExpect or answerClose on the server side as a response.
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @return the message received from the server in response to this message
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    P2LMessage initExpect(MessageEncoder message) throws IOException, TimeoutException;
    /**
     * On server or client after a previous initExpect or answerExpect on the other side.
     * Strictly requires a answerExpect or answerClose on the other side as a response.
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @return the message received from the other side in response to this message
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    P2LMessage answerExpect(MessageEncoder message) throws IOException, TimeoutException;
    /**
     * On server or client after a previous initExpect or answerExpect on the other side.
     * Strictly requires a close() on the other side as a response and receipt that the message sent was correctly received.
     * This operation can be the last on either convo side. No further calls to the conversation object shall be done after calling this method.
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    void answerClose(MessageEncoder message) throws IOException, TimeoutException;



    /**
     * Can only be used by client as the very first statement.
     * (server pauses conversation [client pausing works analogous])
     *    client: val m1 = convo_client.initExpectAfterPause(m0, timeout=10000)
     *    server: init_thread(m0)
     *    server: convo_server.pause()
     *    server: calculating/io operation/remote query
     *    server: convo_server.answerClose(m1)
     *    client: convo_client.close()
     * @return the message received from the peer after the pause
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @param timeout timeout after which to give up waiting on the peer(if it is 0 or less the method will wait forever if no message is received)
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    P2LMessage initExpectAfterPause(MessageEncoder message, int timeout) throws IOException, TimeoutException;
//    DON'T EVEN ALLOW NOT HAVING A TIMEOUT - IT IS INSANITY
//    /** {@link #initExpectAfterPause(MessageEncoder, int)}, with the timeout set to infinity({@link P2LFuture#ENDLESS_WAIT}) */
//    default P2LMessage initExpectAfterPause(MessageEncoder message) throws IOException, TimeoutException {
//        return answerExpectAfterPause(encoded, P2LFuture.ENDLESS_WAIT);
//    }
    /**
     * Used to give the OTHER PARTY the possibility to pause the conversation and take some time to calculate the result (more than a gives and without unnecessarily retrying).
     * (client pauses conversation [server pausing works analogous])
     *    client: val m1 = convo_client.initExpect(m0, timeout=10000)
     *    server: init_thread(m0)
     *    server: convo_server.answerExpectAfterPause(m1)
     *    client: convo_client.pause()
     *    client: calculating/io operation/remote query
     *    client: convo_client.answerClose(m1)
     *    server: convo_server.close()
     * @return the message received from the peer after the pause
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @param timeout timeout after which to give up waiting on the peer(if it is 0 or less the method will wait forever if no message is received)
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    P2LMessage answerExpectAfterPause(MessageEncoder message, int timeout) throws IOException, TimeoutException;
//    DON'T EVEN ALLOW NOT HAVING A TIMEOUT - IT IS INSANITY
//    /** {@link #answerExpectAfterPause(MessageEncoder, int)}, with the timeout set to infinity({@link P2LFuture#ENDLESS_WAIT}) */
//    default P2LMessage answerExpectAfterPause(MessageEncoder message) throws IOException, TimeoutException {
//        return answerExpectAfterPause(encoded, P2LFuture.ENDLESS_WAIT);
//    }
    /**
     * Used by the client or server of a conversation to acknowledge the receival of the message before a pause for calculation.
     * Always used by the party THAT DOES THE CALCULATION.
     * After pausing a conversation the PARTY THAT PAUSED IT continues the conversation by sending a new message,
     * either using {@link #answerExpect(MessageEncoder)} or {@link #answerClose(MessageEncoder)}.
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    void pause() throws IOException, TimeoutException;

    /**
     * Attempts to call {@link #pause()} if that fails, because an exception is thrown nothing is done.
     * This is useful in async call cascades.
     * This is safe, because ,,,, todo it isn't?!
     * @return whether close was successful.
     */
    default boolean tryPause() {
        try {
            pause();
            return true;
        } catch (IOException e) {
            return false;
        }
    }



    /**
     * SPECIAL:: ONLY USE AS FIRST CONVO INSTRUCTION ON CLIENT SIDE WITH RESPONSE BEING 'closeWith'!!!
     *
     * EXAMPLE (ultra short conversation shortcut - closeWith | a better alternative to example 2)
     *    client: val m1 = initExpectClose(m0)
     *    server: init_thread(m0)
     *    server: closeWith(m1)
     *
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @return the message received from the other side in response to this message
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    P2LMessage initExpectClose(MessageEncoder message) throws IOException, TimeoutException;
    /**
     * SPECIAL:: ONLY USE AS FIRST CONVO INSTRUCTION ON SERVER SIDE WITH THE OTHER SIDE HAVING INITIATED THE CONVERSATION WITH 'initExpectClose'!!!
     *
     * EXAMPLE (ultra short conversation shortcut - closeWith | a better alternative to example 2)
     *    client: val m1 = initExpectClose(m0)
     *    server: init_thread(m0)
     *    server: closeWith(m1)
     *
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @throws IOException if the message cannot be send
     */
    void closeWith(MessageEncoder message) throws IOException;

    /**
     * Special, for single safe transfer of one message. Feature overlap with {@link jokrey.utilities.network.link2peer.P2LNode#sendMessageWithRetries(P2Link, P2LMessage, int)}
     *
     * Can be used to send commands to the server,
     * but they may come in multiple times and an alternative is to simply use a message listener without conversations...
     * Example(ONLY possible use):
     *      client: initClose(m0)
     *      server: init_thread(m0)
     *      server: close()
     *
     * HAS to be first and last on client side - needs a close() on server side
     *
     * @param message message to send, its offset has to match {@link #getHeaderSize()}
     * @throws IOException if the message cannot be send
     * @throws TimeoutException if there is no response after (maxAttempts * (m * avRTT + a) + triangularNumber(maxAttempts)*rM) milliseconds
     */
    @Deprecated void initClose(MessageEncoder message) throws IOException, TimeoutException;




    //empty init shortcuts
    byte[] EMPTY_BYTES = new byte[0];
    /** When the client wants to initiate a conversation, but does not have anything to say. */
    default byte[] initExpect() throws IOException, TimeoutException {
        return initExpectData(EMPTY_BYTES);
    }
    /** When the client wants to initiate a conversation, but does not have anything to say. AND additionally wants to close the conversation right away. */
    default byte[] initExpectClose() throws IOException, TimeoutException {   //does not make sense for answer - if you answer with nothing you did something wrong..
        return initExpectDataClose(EMPTY_BYTES);
    }



    //special byte shortcuts

    /** bytes using shortcut for {@link #initExpect(MessageEncoder)} */
    default P2LMessage initExpect(byte[] bytes) throws IOException, TimeoutException { return initExpect(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #answerExpect(MessageEncoder)} */
    default P2LMessage answerExpect(byte[] bytes) throws IOException, TimeoutException { return answerExpect(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #answerExpectAfterPause(MessageEncoder, int)} */
    default P2LMessage answerExpectAfterPause(byte[] bytes, int timeout) throws IOException, TimeoutException { return answerExpectAfterPause(MessageEncoder.from(getHeaderSize(), bytes), timeout); }
    /** bytes using shortcut for {@link #answerClose(MessageEncoder)} */
    default void answerClose(byte[] bytes) throws IOException, TimeoutException { answerClose(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #initClose(MessageEncoder)} */
    @Deprecated default void initClose(byte[] bytes) throws IOException, TimeoutException { initClose(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #initExpectClose(MessageEncoder)} */
    default P2LMessage initExpectClose(byte[] bytes) throws IOException, TimeoutException { return initExpectClose(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #closeWith(MessageEncoder)} */
    default void closeWith(byte[] bytes) throws IOException, TimeoutException { closeWith(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #answerExpectData(MessageEncoder)} */
    default byte[] answerExpectData(MessageEncoder message) throws IOException, TimeoutException { return answerExpect(message).asBytes(); }
    /** bytes using shortcut for {@link #initExpectData(MessageEncoder)} */
    default byte[] initExpectData(MessageEncoder message) throws IOException, TimeoutException { return initExpect(message).asBytes(); }
    /** bytes using shortcut for {@link #initExpectDataClose(MessageEncoder)} */
    default byte[] initExpectDataClose(MessageEncoder message) throws IOException, TimeoutException { return initExpectClose(message).asBytes(); }
    /** bytes using shortcut for {@link #answerExpectData(MessageEncoder)} */
    default byte[] answerExpectData(byte[] bytes) throws IOException, TimeoutException { return answerExpectData(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #initExpectData(MessageEncoder)} */
    default byte[] initExpectData(byte[] bytes) throws IOException, TimeoutException { return initExpectData(MessageEncoder.from(getHeaderSize(), bytes)); }
    /** bytes using shortcut for {@link #initExpectDataClose(MessageEncoder)} */
    default byte[] initExpectDataClose(byte[] bytes) throws IOException, TimeoutException { return initExpectDataClose(MessageEncoder.from(getHeaderSize(), bytes)); }











    void initExpectLong(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException;
    void answerExpectLong(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException;
    P2LMessage longAnswerExpect(TransparentBytesStorage messageSource, int timeout) throws IOException;
    void longAnswerClose(TransparentBytesStorage messageSource, int timeout) throws IOException;

    void initExpectLongAfterPause(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException;
    void answerExpectLongAfterPause(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException;

    void longInitExpectLong(TransparentBytesStorage messageSource, TransparentBytesStorage messageTarget, int timeout) throws IOException;
    void longAnswerExpectLong(TransparentBytesStorage messageSource, TransparentBytesStorage messageTarget, int timeout) throws IOException;





    P2LFuture<P2LMessage> initExpectAsync(MessageEncoder message);
    P2LFuture<P2LMessage> answerExpectAsync(MessageEncoder message);
    P2LFuture<Boolean> answerCloseAsync(MessageEncoder message);
    P2LFuture<P2LMessage> initExpectAsyncAfterPause(MessageEncoder message, int timeout);
    P2LFuture<P2LMessage> answerExpectAsyncAfterPause(MessageEncoder message, int timeout);
}