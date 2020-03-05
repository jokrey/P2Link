import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.stream.P2LInputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LOrderedInputStreamImplV1;
import jokrey.utilities.network.link2peer.node.stream.P2LOrderedOutputStreamImplV1;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.network.link2peer.P2LMessage.MAX_UDP_PACKET_SIZE;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_CONVERSATION_ID;
import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jokrey
 */
public class V1OrderedStreamTests {
    @Test void streamTest_inOut_largeArray_usageAsIntended() throws IOException {
        DebugStats.reset();

//        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =4096;
        int oldLimit = P2LMessage.CUSTOM_RAW_SIZE_LIMIT;
        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = MAX_UDP_PACKET_SIZE;
//        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = 8192*2;
//        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = 8192;
//        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = 4096;
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);

        boolean successConnect = nodes[0].establishConnection(nodes[1].getSelfLink()).get(5000); //TODO TOO SLOW FOR SOME VERY COMPLEX REASON
        assertTrue(successConnect);

        P2LOrderedInputStreamImplV1 in = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 5, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 5, NO_CONVERSATION_ID, in);
        P2LOrderedOutputStreamImplV1 out = new P2LOrderedOutputStreamImplV1((P2LNodeInternal) nodes[1], nodes[0].getSelfLink().getSocketAddress(), P2LMessage.CUSTOM_RAW_SIZE_LIMIT, (short) 5, (short) 0);
        nodes[1].registerCustomOutputStream(nodes[0].getSelfLink().getSocketAddress(), 5, NO_CONVERSATION_ID, out);

//        byte[] toSend = new byte[10_000];//10kb
        byte[] toSend = new byte[100_000_000];//100mb
        ThreadLocalRandom.current().nextBytes(toSend);

        P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
            out.write(toSend);
            out.close();
            System.out.println("end send task");
        });
        System.out.println("1");

        ByteArrayStorage store = new ByteArrayStorage();
        System.out.println("2");
        store.set(0, in, toSend.length);
        System.out.println("3");

        assertArrayEquals(toSend, store.getContent());

        System.out.println("before send task");
        sendTask.waitForIt();
        System.out.println("after send task");

        IntermediateTests.close(nodes);

        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = oldLimit;
//        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;

        DebugStats.printAndReset();
    }

    @Test void streamTest_orderGuaranteed() throws IOException {
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =4;
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62820);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 1, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, NO_CONVERSATION_ID, (P2LInputStream) stream);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";

        new Thread(() -> {
            try {

                int packetCount = P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE -1;
                List<P2LMessage> randomlySplit = IntermediateTests.toBytesAndSplitRandomly(toSend, packetCount);

                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(nodes[0].getSelfLink(), m);
                    sleep(500);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        IntermediateTests.streamSplitAssertions(stream, toSend, false);

        IntermediateTests.close(nodes);
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =128;
    }

    @Test void streamTest_orderNotGuaranteed_guaranteedFewerThanBufferPacketsSend_noDrops() throws IOException {
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =128;
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62830);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 1, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, NO_CONVERSATION_ID, (P2LInputStream) stream);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int packetCount = P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE -1;
                List<P2LMessage> randomlySplit = IntermediateTests.toBytesAndSplitRandomly(toSend, packetCount);
                Collections.shuffle(randomlySplit);

                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(nodes[0].getSelfLink(), m);
                    sleep(5);//form of congestion control
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        IntermediateTests.streamSplitAssertions(stream, toSend, false);

        IntermediateTests.close(nodes);
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =128;
    }

    @Test void streamTest_sendOrderReverse_guaranteedFewerThanBufferPacketsSend_noDrops() throws IOException {
        DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;

        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62840);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 1, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, NO_CONVERSATION_ID, (P2LInputStream) stream);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int packetCount = P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE -1;
                List<P2LMessage> randomlySplit = IntermediateTests.toBytesAndSplitRandomly(toSend, packetCount);
                Collections.reverse(randomlySplit);

                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(nodes[0].getSelfLink(), m);
                    sleep(5);//form of congestion control
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        IntermediateTests.streamSplitAssertions(stream, toSend, false);

        IntermediateTests.close(nodes);
    }

    @Test
    void streamTest_orderGuaranteed_twiceThanBufferSizePacketsSend_noDrops() throws IOException {
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =4;

        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62850);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 1, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, NO_CONVERSATION_ID, (P2LInputStream) stream);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int packetCount = P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE *3;
                List<P2LMessage> randomlySplit = IntermediateTests.toBytesAndSplitRandomly(toSend, packetCount);
//                Collections.reverse(randomlySplit);

                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(nodes[0].getSelfLink(), m);
                    sleep(500);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        IntermediateTests.streamSplitAssertions(stream, toSend, false);

        IntermediateTests.close(nodes);

        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =128;
    }

    @Test void streamTest_orderGuaranteedUpUntilBufferSize_twiceThanBufferSizePacketsSend_noDrops() throws IOException {
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =4;
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62860);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 1, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, NO_CONVERSATION_ID, (P2LInputStream) stream);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int partsCount = 3;
                int packetCount = P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE *partsCount;
                List<P2LMessage> randomlySplit = IntermediateTests.toBytesAndSplitRandomly(toSend, packetCount);
                List<List<P2LMessage>> parts = new ArrayList<>();
                for(int i=0;i<partsCount;i++)
                    parts.add(new ArrayList<>(P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE));

                int messagesSend = 0;
                int inPart = 0;
                for(P2LMessage m:randomlySplit) {
                    parts.get(inPart).add(m);
                    messagesSend++;
                    if(messagesSend%(P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE)==0) {
                        Collections.shuffle(parts.get(inPart));
                        inPart++;
                    }
                }

                for(List<P2LMessage> messages:parts)
                    for(P2LMessage message:messages) {
                        nodes[1].sendMessage(nodes[0].getSelfLink(), message);
                        sleep(5);//form of congestion control
                    }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        IntermediateTests.streamSplitAssertions(stream, toSend, false);

        IntermediateTests.close(nodes);
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =128;
    }

    @Test @Disabled
    void streamTest_orderNotGuaranteed_twiceThanBufferSizePacketsSend_noDrops() throws IOException {
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =4;
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62870);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), (short) 1, (short) 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, NO_CONVERSATION_ID, (P2LInputStream) stream);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {

                int packetCount = P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE *3;
                List<P2LMessage> randomlySplit = IntermediateTests.toBytesAndSplitRandomly(toSend, packetCount);
                Collections.shuffle(randomlySplit);
//                Collections.reverse(randomlySplit);

//                int messagesSend = 0;
                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(nodes[0].getSelfLink(), m);
                    sleep(5);

//                    messagesSend++;
//                    if(messagesSend%(P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE)==0)
//                        sleep(250);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        IntermediateTests.streamSplitAssertions(stream, toSend, false);

        IntermediateTests.close(nodes);
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =128;
    }
}
