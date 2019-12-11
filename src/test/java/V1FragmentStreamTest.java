import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentInputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentOutputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentOutputStreamImplV1;
import jokrey.utilities.network.link2peer.node.stream.fragment.*;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.network.link2peer.P2LMessage.MAX_UDP_PACKET_SIZE;
import static jokrey.utilities.network.link2peer.util.P2LFuture.ENDLESS_WAIT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jokrey
 */
public class V1FragmentStreamTest {
    //todo - test multiple, simultaneous streams...

    @Test void testFragmentStreamLargeSource_noIntentionalDrops_maxUdpSize_strikeDown() throws IOException, InterruptedException {
        testFragmentStream(0, 100_000_000, MAX_UDP_PACKET_SIZE,
                new LossAcceptabilityCalculator_Packages(4),
                new BatchSizeCalculator_StrikeDown(256, 32, 8).creator());
    }
    @Test void testFragmentStreamLargeSource_noIntentionalDrops_maxUdpSize_DynamicBms() throws IOException, InterruptedException {
        testFragmentStream(0, 100_000_000, MAX_UDP_PACKET_SIZE,
                new LossAcceptabilityCalculator_Packages(4),
                new BatchSizeCalculator_DynamicBMS(256, 2, 1.001f, 0.05f, 0.001f).creator());
    }
    @Test void testFragmentStreamLargeSource_noIntentionalDrops_strikeDown() throws IOException, InterruptedException {
        testFragmentStream(0, 100_000_000, -1,
                new LossAcceptabilityCalculator_Packages(4),
                new BatchSizeCalculator_StrikeDown(128, 32, 8).creator());
    }
    @Test void testFragmentStreamLargeSource_noIntentionalDrops_DynamicBms() throws IOException, InterruptedException {
        testFragmentStream(0, 100_000_000, -1,
                new LossAcceptabilityCalculator_Packages(4),
                new BatchSizeCalculator_DynamicBMS(128, 2, 1.001f, 0.05f, 0.001f).creator());
    }
//    @Test void testFragmentStreamLargeSource_noIntentionalDrops_maxUdpSize_StaticBatchSize300() throws IOException, InterruptedException {
//        testFragmentStream(0, 100_000_000, MAX_UDP_PACKET_SIZE,
//                new LossAcceptabilityCalculator_Packages(4),
//                new BatchSizeCalculator_Static(300).creator());
//    }
//    @Test void testFragmentStreamLargeSource_noIntentionalDrops_maxUdpSize_StaticBatchSize50() throws IOException, InterruptedException {
//        testFragmentStream(0, 100_000_000, MAX_UDP_PACKET_SIZE,
//                new LossAcceptabilityCalculator_Packages(4),
//                new BatchSizeCalculator_Static(50).creator());
//    }
//    @Test void testFragmentStreamLargeSource_noIntentionalDrops_maxUdpSize_StaticBatchSize1000() throws IOException, InterruptedException {
//        testFragmentStream(0, 100_000_000, MAX_UDP_PACKET_SIZE,
//                new LossAcceptabilityCalculator_Packages(4),
//                new BatchSizeCalculator_Static(1000).creator());
//    }
//    @Test void testFragmentStreamLargeSource_noIntentionalDrops_maxUdpSize_StaticBatchSize2500() throws IOException, InterruptedException {
//        testFragmentStream(0, 100_000_000, MAX_UDP_PACKET_SIZE,
//                new LossAcceptabilityCalculator_Packages(4),
//                new BatchSizeCalculator_Static(2500).creator());
//    }
    void testFragmentStream(int dropPercentage, int numBytesToSend, int packetSizeLimit, LossAcceptabilityCalculator lossDetectionScheme, BatchSizeCalculatorCreator batchSizeScheme) throws IOException, InterruptedException {
        DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = dropPercentage;
        int oldLimit = P2LMessage.CUSTOM_RAW_SIZE_LIMIT;
        BatchSizeCalculatorCreator oldBatchSizeScheme = P2LFragmentOutputStreamImplV1.batchSizeCalculatorCreator;
        LossAcceptabilityCalculator oldLossDetectionScheme = P2LFragmentOutputStreamImplV1.lossConverter;
        if(packetSizeLimit!=-1)
            P2LMessage.CUSTOM_RAW_SIZE_LIMIT = packetSizeLimit;
        P2LFragmentOutputStreamImplV1.batchSizeCalculatorCreator = batchSizeScheme;
        P2LFragmentOutputStreamImplV1.lossConverter = lossDetectionScheme;
        DebugStats.reset();

        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62890);
        try {
            P2LFuture<Boolean> connectionEstablished = nodes[0].establishConnection(nodes[1].getSelfLink());

            //utilizing the thread, while it waits - async for DA win
            byte[] toSend = new byte[numBytesToSend];
            ThreadLocalRandom.current().nextBytes(toSend);

            assertTrue(connectionEstablished.get(10000));

            TransparentBytesStorage source = new ByteArrayStorage(toSend);
            P2LFragmentInputStream in = nodes[0].createFragmentInputStream(nodes[1].getSelfLink(), 555, P2LNode.NO_CONVERSATION_ID);
            P2LFragmentOutputStream out = nodes[1].createFragmentOutputStream(nodes[0].getSelfLink(), 555, P2LNode.NO_CONVERSATION_ID);
            out.setSource(toSend);


            TimeDiffMarker.setMark("send data");
            TransparentBytesStorage target = new ByteArrayStorage(toSend.length);
            in.writeResultsTo(target);

            out.send();
            out.close();
            TimeDiffMarker.println("send data");

            assertTrue(in.isFullyReceived());

            assertArrayEquals(source.getContent(), target.getContent());
        } finally {
            IntermediateTests.close(nodes);
            DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
            P2LMessage.CUSTOM_RAW_SIZE_LIMIT = oldLimit;
            P2LFragmentOutputStreamImplV1.batchSizeCalculatorCreator = oldBatchSizeScheme;
            P2LFragmentOutputStreamImplV1.lossConverter = oldLossDetectionScheme;
            DebugStats.printAndReset();
        }
    }




    @Test void testFragmentStreamCompareSpeedWithAndWithoutEstablishedConnection() throws IOException, InterruptedException {
        DebugStats.reset();

        int bufferSize = 1_000_000;//1000kb=1mb
        {
            P2LNode[] nodes = IntermediateTests.generateNodes(2, 62890);

            nodes[0].establishConnection(nodes[1].getSelfLink()).waitForIt(10000);

            byte[] toSend = new byte[bufferSize];
            ThreadLocalRandom.current().nextBytes(toSend);

            TimeDiffMarker.setMark_d();
            P2LFragmentInputStream in = nodes[0].createFragmentInputStream(nodes[1].getSelfLink(), 555, P2LNode.NO_CONVERSATION_ID);
            P2LFragmentOutputStream out = nodes[1].createFragmentOutputStream(nodes[0].getSelfLink(), 555, P2LNode.NO_CONVERSATION_ID);

            out.setSource(toSend);
            out.send();
            out.waitForConfirmationOnAll(ENDLESS_WAIT);
            TimeDiffMarker.println_d("took: ");
            IntermediateTests.close(nodes);
        }

        DebugStats.printAndReset();

        {
            P2LNode[] nodes = IntermediateTests.generateNodes(2, 62890);

//            nodes[0].establishConnection(nodes[1].getSelfLink()).waitForIt(1000);

            byte[] toSend = new byte[bufferSize];
            ThreadLocalRandom.current().nextBytes(toSend);

            TimeDiffMarker.setMark_d();
            TransparentBytesStorage source = new ByteArrayStorage(toSend);
            P2LFragmentInputStream in = nodes[0].createFragmentInputStream(nodes[1].getSelfLink(), 555, P2LNode.NO_CONVERSATION_ID);
            P2LFragmentOutputStream out = nodes[1].createFragmentOutputStream(nodes[0].getSelfLink(), 555, P2LNode.NO_CONVERSATION_ID);

            out.setSource(toSend);
            out.send();
            out.waitForConfirmationOnAll(ENDLESS_WAIT);
            TimeDiffMarker.println_d("took: ");
            IntermediateTests.close(nodes);
        }

        DebugStats.printAndReset();
    }

}
