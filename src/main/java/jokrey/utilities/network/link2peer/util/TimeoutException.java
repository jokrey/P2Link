package jokrey.utilities.network.link2peer.util;

/**
 * Thrown by the blocking wait methods of P2LFuture, when the timeout is reached
 * @author jokrey
 */
public class TimeoutException extends RuntimeException {
    public TimeoutException() {super();}
    public TimeoutException(String s) {
        super(s);
    }
}
