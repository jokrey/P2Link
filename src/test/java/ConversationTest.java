import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.core.P2LConversation;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author jokrey
 */
public class ConversationTest {
    private static short conversationId = 0;
    @Test void convTest_4m_60PercentDrops_100MaxRetries() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(true, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectData(rm[0]);
                    assertArrayEquals(rm[1], m1);
                    byte[] m3 = convo.answerExpectData(rm[2]);
                    assertArrayEquals(rm[3], m3);
                    convo.answerClose(rm[4]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    byte[] m2 = convo.answerExpectData(rm[1]);
                    assertArrayEquals(rm[2], m2);
                    byte[] m4 = convo.answerExpectData(rm[3]);
                    assertArrayEquals(rm[4], m4);
                    convo.close(); //problem:: if the message send here does not arrive - what is the other side supposed to expect...?? How are retries from the other side handled?
                });
    }

    public static byte[] rand(int size) {
        byte[] b = new byte[size];
        ThreadLocalRandom.current().nextBytes(b);
        return b;
    }
    public static byte[][] rand(int num, int size) {
        byte[][] bs = new byte[num][];
        for(int i=0;i<bs.length;i++)
            bs[i] = rand(size);
        return bs;
    }

    @Test void convTest_4m_60PercentDrops_100MaxRetries_conNotEstablished() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(false, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectData(rm[0]);
                    assertArrayEquals(rm[1], m1);
                    byte[] m3 = convo.answerExpectData(rm[2]);
                    assertArrayEquals(rm[3], m3);
                    convo.answerClose(rm[4]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    byte[] m2 = convo.answerExpectData(rm[1]);
                    assertArrayEquals(rm[2], m2);
                    byte[] m4 = convo.answerExpectData(rm[3]);
                    assertArrayEquals(rm[4], m4);
                    convo.close(); //problem:: if the message send here does not arrive - what is the other side supposed to expect...?? How are retries from the other side handled?
                });
    }



    @Test void convTest_firstServerAnswerIsAnswerClose_60Drops_100MaxRetries() throws IOException {
        byte[][] rm = rand(2, 1000);
        testEnvConversation(true, 60, 100, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectData(rm[0]);
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
                    byte[] m1 = convo.initExpectDataClose(rm[0]);
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
                    byte[] m1 = convo.initExpectDataClose(rm[0]);
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


    @Test void convTest_pause() throws IOException {
        P2LNode[] nodes = IntermediateTests.generateNodes(2, 63880);
        P2LNode x = nodes[0];
        P2LNode y = nodes[1];

        y.registerConversationFor(1, (convo, m0) -> {
            if(! m0.asString().equals("hallo"))
                return;
            P2LMessage result = convo.initExpect(convo.encode("hallo"));
            convo.pause();
            int a1 = result.nextInt();
            int a2 = result.nextInt();

            int calculated = a1 + a2;
            sleep(10000);

            convo.answerClose(convo.encode(calculated));
        });

        P2LConversation convo = x.convo(1, y.getSelfLink());
        String handshakeAnswer = convo.initExpect(convo.encode("hallo")).asString();
        if(! handshakeAnswer.equals("hallo")) fail();

        int result = convo.answerExpectAfterPause(convo.encode(1, 5)).nextInt();
        convo.close();
        if(result != 6) fail();

        //  y: (a1, a2) = convo.initExpect("hallo")
        //  x: result = convo.answerPause(convo.encode(1, 2), timeout=20minutes)
        //  y: convo.pause()
        //  y: calculated = a1 + a2 //takes 10 minutes
        //  y: convo.answerClose(calculated)
        //  x: result was set to calculated
        //  x: convo.close()
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
