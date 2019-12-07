import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.stream.P2LInputStream;
import jokrey.utilities.network.link2peer.core.stream.P2LOrderedInputStreamImplV1;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;

/**
 * @author jokrey
 */
public class V1OrderedStreamTests {

    @Test void streamTest_orderGuaranteed() throws IOException {
        P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE =4;
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62820);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), 1, 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, P2LNode.NO_CONVERSATION_ID, (P2LInputStream) stream);

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

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), 1, 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, P2LNode.NO_CONVERSATION_ID, (P2LInputStream) stream);

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
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62840);

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), 1, 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, P2LNode.NO_CONVERSATION_ID, (P2LInputStream) stream);

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

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), 1, 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, P2LNode.NO_CONVERSATION_ID, (P2LInputStream) stream);

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

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), 1, 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, P2LNode.NO_CONVERSATION_ID, (P2LInputStream) stream);

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

        InputStream stream = new P2LOrderedInputStreamImplV1((P2LNodeInternal) nodes[0], nodes[1].getSelfLink().getSocketAddress(), 1, 0);
        nodes[0].registerCustomInputStream(nodes[1].getSelfLink().getSocketAddress(), 1, P2LNode.NO_CONVERSATION_ID, (P2LInputStream) stream);

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
