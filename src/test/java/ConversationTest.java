import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConversation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jokrey
 */
public class ConversationTest {
    private static short conversationId = 0;
    @Test void convTest1() throws IOException {
//        DebugStats.reset();

        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);
        try {

            DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
            boolean successConnect = nodes[0].establishConnection(nodes[1].getSelfLink()).get(1000);
            assertTrue(successConnect);

            DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 5;

            System.out.println("\n\n\n\n\n");
            AtomicBoolean bool = new AtomicBoolean(false);

//            byte[] rm0 = {0};
//            byte[] rm1 = {1};
//            byte[] rm2 = {2};
//            byte[] rm3 = {3};
//            byte[] rm4 = {4};
            byte[] rm0 = new byte[1000];
            byte[] rm1 = new byte[1000];
            byte[] rm2 = new byte[1000];
            byte[] rm3 = new byte[1000];
            byte[] rm4 = new byte[1000];
            ThreadLocalRandom.current().nextBytes(rm0);
            ThreadLocalRandom.current().nextBytes(rm1);
            ThreadLocalRandom.current().nextBytes(rm2);
            ThreadLocalRandom.current().nextBytes(rm3);
            ThreadLocalRandom.current().nextBytes(rm4);

            /*m0*/
            nodes[1].registerConversationFor(25, (convo, m0) -> {
                assertArrayEquals(rm0, m0.asBytes());
                byte[] m2 = convo.answerExpect(rm1);
                assertArrayEquals(rm2, m2);
                byte[] m4 = convo.answerExpect(rm3);
                assertArrayEquals(rm4, m4);
                bool.set(true);
                convo.close(); //problem:: if the message send here does not arrive - what is the other side supposed to expect...?? How are retries from the other side handled?
            });

            TimeDiffMarker.setMark("convo");
            P2LConversation convo = nodes[0].convo(25, conversationId++, nodes[1].getSelfLink());
            byte[] m1 = convo.initExpect(rm0);
            byte[] m3 = convo.answerExpect(rm2);
            convo.answerClose(rm4);

            TimeDiffMarker.println("convo");


            assertArrayEquals(rm1, m1);
            assertArrayEquals(rm3, m3);
            assertTrue(bool.get());

        } finally {
            IntermediateTests.close(nodes);
            sleep(10);

//            DebugStats.printAndReset();
            DebugStats.print();
        }
    }
}
