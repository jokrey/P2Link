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

    public static AtomicInteger conversation_numRetries = new AtomicInteger(0);
    public static AtomicInteger conversation_numValid = new AtomicInteger(0);

    public static int INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
    public static AtomicInteger incomingHandler_numIntentionallyDropped = new AtomicInteger(0);

    public static boolean MSG_PRINTS_ACTIVE = false;

    public static void reset() {
        MSG_PRINTS_ACTIVE = false;
        fragmentStream1_numResend.set(0);
        fragmentStream1_doubleReceived.set(0);
        fragmentStream1_validReceived.set(0);
        orderedStream1_numResend.set(0);
        orderedStream1_doubleReceived.set(0);
        orderedStream1_validReceived.set(0);
        incomingHandler_numStreamReceipts.set(0);
        incomingHandler_numStreamParts.set(0);
        incomingHandler_numIntentionallyDropped.set(0);
        conversation_numRetries.set(0);
        conversation_numValid.set(0);
        INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE=0;
    }

    public static void print() {
        print(false);
    }
    public static void print(boolean filterUnchanged) {
        if(!filterUnchanged || MSG_PRINTS_ACTIVE)
            System.out.println("MSG_PRINTS_ACTIVE = " + MSG_PRINTS_ACTIVE);
        if(!filterUnchanged || INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE!=0)
            System.out.println("INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = " + INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE);
        if(!filterUnchanged || incomingHandler_numIntentionallyDropped.get()!=0)
            System.out.println("incomingHandler_numIntentionallyDropped = " + incomingHandler_numIntentionallyDropped);
        if(!filterUnchanged || incomingHandler_numStreamParts.get()!=0)
            System.out.println("incomingHandler_numStreamParts = " + incomingHandler_numStreamParts);
        if(!filterUnchanged || incomingHandler_numStreamReceipts.get()!=0)
            System.out.println("incomingHandler_numStreamReceipts = " + incomingHandler_numStreamReceipts);
        if(!filterUnchanged || fragmentStream1_numResend.get()!=0)
            System.out.println("fragmentStream1_numResend = " + fragmentStream1_numResend);
        if(!filterUnchanged || fragmentStream1_doubleReceived.get()!=0)
            System.out.println("fragmentStream1_doubleReceived = " + fragmentStream1_doubleReceived);
        if(!filterUnchanged || fragmentStream1_validReceived.get()!=0)
            System.out.println("fragmentStream1_validReceived = " + fragmentStream1_validReceived);
        if(!filterUnchanged || orderedStream1_numResend.get()!=0)
            System.out.println("orderedStream1_numResend = " + orderedStream1_numResend);
        if(!filterUnchanged || orderedStream1_doubleReceived.get()!=0)
            System.out.println("orderedStream1_doubleReceived = " + orderedStream1_doubleReceived);
        if(!filterUnchanged || orderedStream1_validReceived.get()!=0)
            System.out.println("orderedStream1_validReceived = " + orderedStream1_validReceived);
        if(!filterUnchanged || conversation_numRetries.get()!=0)
            System.out.println("conversation_numRetries = " + conversation_numRetries);
        if(!filterUnchanged || conversation_numValid.get()!=0)
            System.out.println("conversation_numValid = " + conversation_numValid);
    }

    public static void printAndReset() {
        print();
        reset();
    }
}
