import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.core.P2LConversation;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author jokrey
 */
public class ConversationTest {
    private static short conversationId = 0;
    @Test void convTest_4m_60PercentDrops_100MaxRetries() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(true, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpect(rm[0]);
                    assertArrayEquals(rm[1], m1);
                    byte[] m3 = convo.answerExpect(rm[2]);
                    assertArrayEquals(rm[3], m3);
                    convo.answerClose(rm[4]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    byte[] m2 = convo.answerExpect(rm[1]);
                    assertArrayEquals(rm[2], m2);
                    byte[] m4 = convo.answerExpect(rm[3]);
                    assertArrayEquals(rm[4], m4);
                    convo.close(); //problem:: if the message send here does not arrive - what is the other side supposed to expect...?? How are retries from the other side handled?
                });
    }

    private byte[] rand(int size) {
        byte[] b = new byte[size];
        ThreadLocalRandom.current().nextBytes(b);
        return b;
    }
    private byte[][] rand(int num, int size) {
        byte[][] bs = new byte[num][];
        for(int i=0;i<bs.length;i++)
            bs[i] = rand(size);
        return bs;
    }

    @Test void convTest_4m_60PercentDrops_100MaxRetries_conNotEstablished() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(false, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpect(rm[0]);
                    assertArrayEquals(rm[1], m1);
                    byte[] m3 = convo.answerExpect(rm[2]);
                    assertArrayEquals(rm[3], m3);
                    convo.answerClose(rm[4]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    byte[] m2 = convo.answerExpect(rm[1]);
                    assertArrayEquals(rm[2], m2);
                    byte[] m4 = convo.answerExpect(rm[3]);
                    assertArrayEquals(rm[4], m4);
                    convo.close(); //problem:: if the message send here does not arrive - what is the other side supposed to expect...?? How are retries from the other side handled?
                });
    }



    @Test void convTest_firstServerAnswerIsAnswerClose_60Drops_100MaxRetries() throws IOException {
        byte[][] rm = rand(2, 1000);
        testEnvConversation(true, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpect(rm[0]);
                    assertArrayEquals(rm[1], m1);
                    convo.close();
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.answerClose(rm[1]);
                });
    }



    @Test void convTest_serverDirectClose_60Drops_100MaxRetries() throws IOException {
        byte[][] rm = rand(2, 1000);
        testEnvConversation(true, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectClose(rm[0]);
                    assertArrayEquals(rm[1], m1);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.closeWith(rm[1]);
                });
    }
    @Test void convTest_serverDirectClose_9Drops_1Retry() throws IOException {
        byte[][] rm = rand(2, 1000);
        testEnvConversation(true, 0, 1, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectClose(rm[0]);
                    assertArrayEquals(rm[1], m1);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.closeWith(rm[1]);
                });
    }

    @Test void convTest_clientDirectClose_60Drops_100MaxRetries() throws IOException {
        byte[][] rm = rand(1, 1000);
        testEnvConversation(true, 60, 100, false,
                (convo, no) -> {//client
                     convo.initClose(rm[0]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.close();
                });
    }



    private static void testEnvConversation(boolean connect, int dropPercentage, int retries, boolean resetDebug, ConversationAnswererChangeThisName clientLogic, ConversationAnswererChangeThisName serverLogic) throws IOException {
        if(resetDebug)DebugStats.reset();

        P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);
        try {

            if(connect) {
                DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
                DebugStats.MSG_PRINTS_ACTIVE = false;
                boolean successConnect = nodes[0].establishConnection(nodes[1].getSelfLink()).get(1000);
                assertTrue(successConnect);
            }

            DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = dropPercentage;
            DebugStats.MSG_PRINTS_ACTIVE = true;

            P2LFuture<Boolean> bool = new P2LFuture<>();

            /*m0*/
            nodes[1].registerConversationFor(25, (convo, m0) -> {
                convo.setMaxAttempts(retries);
                serverLogic.converse(convo, m0);
                bool.setCompleted(true);
            });

            TimeDiffMarker.setMark("convo");
            P2LConversation convo = nodes[0].convo(25, nodes[1].getSelfLink());
            convo.setMaxAttempts(retries);
            clientLogic.converse(convo, null);

            TimeDiffMarker.println("convo");
            
            assertTrue(bool.get(2500));

            TimeDiffMarker.println("convo");

        } finally {
            IntermediateTests.close(nodes);
            sleep(10);
            DebugStats.print();
            if(resetDebug) DebugStats.reset();
        }
    }
}
