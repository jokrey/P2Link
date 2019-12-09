import jokrey.utilities.debug_analysis_helper.AverageCallTimeMarker;
import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.core.DebugStats;
import jokrey.utilities.network.link2peer.core.stream.P2LFragmentInputStream;
import jokrey.utilities.network.link2peer.core.stream.P2LFragmentOutputStream;
import jokrey.utilities.network.link2peer.core.stream.P2LOrderedInputStream;
import jokrey.utilities.network.link2peer.core.stream.P2LOrderedOutputStream;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.network.link2peer.P2LMessage.MAX_UDP_PACKET_SIZE;
import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author jokrey
 */
public class V2OrderedStreamTests {
    @Test void streamTest_inOut_largeArray_usageAsIntended() throws IOException {
        DebugStats.reset();
        int oldLimit = P2LMessage.CUSTOM_RAW_SIZE_LIMIT;
        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = MAX_UDP_PACKET_SIZE;

        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);
        try {

            boolean successConnect = nodes[0].establishConnection(nodes[1].getSelfLink()).get(1000); //TODO TOO SLOW FOR SOME VERY COMPLEX REASON
            assertTrue(successConnect);

            InputStream in = nodes[0].createInputStream(nodes[1].getSelfLink(), 5, P2LNode.NO_CONVERSATION_ID);
            OutputStream out = nodes[1].createOutputStream(nodes[0].getSelfLink(), 5, P2LNode.NO_CONVERSATION_ID);

//        byte[] toSend = new byte[1_000_000];//10kb
            byte[] toSend = new byte[100_000_000];//100mb
            ThreadLocalRandom.current().nextBytes(toSend);

            P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
                out.write(toSend);
                out.close();
            });

            ByteArrayStorage store = new ByteArrayStorage();
            store.set(0, in, toSend.length);

            AverageCallTimeMarker.print_all();

            assertArrayEquals(toSend, store.getContent());

            System.out.println("before send task");
            sendTask.waitForIt();
            System.out.println("after send task");
        } finally {
            IntermediateTests.close(nodes);
            P2LMessage.CUSTOM_RAW_SIZE_LIMIT = oldLimit;

            DebugStats.printAndReset();
        }
    }








    @Test void streamTest_inOut_stringScanner() throws IOException {
        DebugStats.reset();
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);

        try {

            InputStream in = nodes[0].createInputStream(nodes[1].getSelfLink(), 1, P2LNode.NO_CONVERSATION_ID);
            OutputStream out = nodes[1].createOutputStream(nodes[0].getSelfLink(), 1, P2LNode.NO_CONVERSATION_ID);

            String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";

            P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
                List<P2LMessage> split = IntermediateTests.toBytesAndSplitRandomly(toSend, 8);
                for (P2LMessage s : split) {
                    System.out.println("s.asString() = " + s.asString());
                    out.write(s.asBytes());
                    out.flush(); //otherwise it is only internally buffered until the buffer is full or it is closed(or flushed like demoed here)
                }
//                out.flush();//  works also, but close does an internal, direct eof flush
                out.close();
            });

            IntermediateTests.streamSplitAssertions(in, toSend, false);

            sendTask.waitForIt();

            in.close();
        } finally {
            IntermediateTests.close(nodes);
            DebugStats.printAndReset();
        }
    }



    @Test void streamTest_closingOutFirst() throws IOException {
        DebugStats.reset();
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);

        try {
            P2LOrderedInputStream in = nodes[0].createInputStream(nodes[1].getSelfLink(), 2, P2LNode.NO_CONVERSATION_ID);
            P2LOrderedOutputStream out = nodes[1].createOutputStream(nodes[0].getSelfLink(), 2, P2LNode.NO_CONVERSATION_ID);

            P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
                out.write(new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16});
                out.close();
            });

            System.out.println(1);
            byte[] read = new byte[5];
            int numRead = in.read(read);
            sendTask.waitForIt(); //data still available to read after out closed...

            System.out.println(2);

            assertEquals(5, numRead);
            assertArrayEquals(new byte[] {1,2,3,4,5}, read);

            read = new byte[11];
            numRead = in.read(read);
            assertEquals(11, numRead);
            System.out.println("read = " + Arrays.toString(read));
            assertArrayEquals(new byte[] {6,7,8,9,10,11,12,13,14,15,16}, read);

            boolean inClosed = in.isClosed();
            boolean outClosed = out.isClosed();
            assertTrue(inClosed);
            assertTrue(outClosed);

        } finally {
            IntermediateTests.close(nodes);
            DebugStats.printAndReset();
        }
    }

    @Test void streamTest_closingInFirst() throws IOException {
        DebugStats.reset();
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);

        try {
            P2LOrderedInputStream in = nodes[0].createInputStream(nodes[1].getSelfLink(), 3, P2LNode.NO_CONVERSATION_ID);
            P2LOrderedOutputStream out = nodes[1].createOutputStream(nodes[0].getSelfLink(), 3, P2LNode.NO_CONVERSATION_ID);

            P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
                out.write(new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16});
                out.flush();

                sleep(500);
    //            out.close();
            });

            byte[] read = new byte[5];
            int numRead = in.read(read);

            assertEquals(5, numRead);
            assertArrayEquals(new byte[] {1,2,3,4,5}, read);

            assertFalse(sendTask.isCompleted());

            in.close();

            sendTask.waitForIt();

            assertThrows(IOException.class, ()-> in.read(read));

            boolean inClosed = in.isClosed();
            boolean outClosed = out.isClosed(); //note that this is despite that fact that out was never closed actively by the sender thread or anyone else.. it received the notification from the in stream
            assertTrue(inClosed);
            assertTrue(outClosed);

            assertThrows(IOException.class, () -> out.write(1));

        } finally {
            IntermediateTests.close(nodes);
            DebugStats.printAndReset();
        }
    }

    @Test void streamTest_inOut_largeArray_usageAsIntended_tcp() throws IOException {
        ServerSocket server = new ServerSocket(6003);
        Socket client = new Socket("localhost", 6003);
        Socket serversConnectionToClient = server.accept();

        InputStream in = serversConnectionToClient.getInputStream();
        OutputStream out = client.getOutputStream();

//        byte[] toSend = new byte[10_000];//10kb
        byte[] toSend = new byte[100_000_000];//10mb
        ThreadLocalRandom.current().nextBytes(toSend);

        P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
            out.write(toSend);

//            sleep(100);

            out.close();
        });

        ByteArrayStorage store = new ByteArrayStorage();
        store.set(0, in, toSend.length);

        assertArrayEquals(toSend, store.getContent());

        sendTask.waitForIt();

        in.close();
        client.close();
        serversConnectionToClient.close();
        server.close();
    }
}
