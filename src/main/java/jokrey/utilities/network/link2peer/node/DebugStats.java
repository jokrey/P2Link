package jokrey.utilities.network.link2peer.node;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jokrey
 */
public class DebugStats {
    public static AtomicInteger fragmentStream1_numResend = new AtomicInteger(0);
    public static AtomicInteger fragmentStream1_doubleReceived = new AtomicInteger(0);
    public static AtomicInteger fragmentStream1_validReceived = new AtomicInteger(0);

    public static AtomicInteger orderedStream1_numResend = new AtomicInteger(0);
    public static AtomicInteger orderedStream1_doubleReceived = new AtomicInteger(0);
    public static AtomicInteger orderedStream1_validReceived = new AtomicInteger(0);

    public static AtomicInteger incomingHandler_numStreamReceipts = new AtomicInteger(0);
    public static AtomicInteger incomingHandler_numStreamParts = new AtomicInteger(0);

    public static int INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
    public static AtomicInteger incomingHandler_numIntentionallyDropped = new AtomicInteger(0);

    public static void reset() {
        fragmentStream1_numResend.set(0);
        fragmentStream1_doubleReceived.set(0);
        fragmentStream1_validReceived.set(0);
        orderedStream1_numResend.set(0);
        orderedStream1_doubleReceived.set(0);
        orderedStream1_validReceived.set(0);
        incomingHandler_numStreamReceipts.set(0);
        incomingHandler_numStreamParts.set(0);
        incomingHandler_numIntentionallyDropped.set(0);
        INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE=0;
    }

    public static void print() {
        System.out.println("INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = " + INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE);
        System.out.println("incomingHandler_numStreamParts = " + incomingHandler_numStreamParts);
        System.out.println("incomingHandler_numStreamReceipts = " + incomingHandler_numStreamReceipts);
        System.out.println("fragmentStream1_numResend = " + fragmentStream1_numResend);
        System.out.println("fragmentStream1_doubleReceived = " + fragmentStream1_doubleReceived);
        System.out.println("fragmentStream1_validReceived = " + fragmentStream1_validReceived);
        System.out.println("orderedStream1_numResend = " + orderedStream1_numResend);
        System.out.println("orderedStream1_doubleReceived = " + orderedStream1_doubleReceived);
        System.out.println("orderedStream1_validReceived = " + orderedStream1_validReceived);
    }

    public static void printAndReset() {
        print();
        reset();
    }
}
