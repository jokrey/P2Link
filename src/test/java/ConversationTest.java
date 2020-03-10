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



    @Test void convTest_4mPingPongServerClose_conEstablished() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(true, 0, 1, false, false,
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
                    convo.close();
                });
    }

    @Test void convTest_4mPingPongServerClose_conNotEstablished() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(false, 0, 1, false, false,
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
                    convo.close();
                });
    }

    @Test void convTest_3mPingPongClientClose_conEstablished() throws IOException {
        byte[][] rm = rand(5, 1000);
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectData(rm[0]);
                    assertArrayEquals(rm[1], m1);
                    byte[] m3 = convo.answerExpectData(rm[2]);
                    assertArrayEquals(rm[3], m3);
                    convo.close();
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    byte[] m2 = convo.answerExpectData(rm[1]);
                    assertArrayEquals(rm[2], m2);
                    convo.answerClose(rm[3]);
                });
    }



    @Test void convTest_firstServerAnswerIsAnswerClose() throws IOException {
        byte[][] rm = rand(2, 1000);

        //this can be more efficient using 'closeWith'
        testEnvConversation(true, 0, 1, false, false,
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



    @Test void convTest_closeWith() throws IOException {
        byte[][] rm = rand(2, 1000);
        testEnvConversation(true, 0, 1, false, true,
                (convo, no) -> {//client
                    byte[] m1 = convo.initExpectDataClose(rm[0]);
                    assertArrayEquals(rm[1], m1);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.closeWith(rm[1]);
                });
    }

    @Test void convTest_initClose() throws IOException {
        byte[][] rm = rand(1, 1000);
        testEnvConversation(true, 0, 1, false, true,
                (convo, no) -> {//client
                     convo.initClose(rm[0]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.close();
                });
    }


    @Test void convTest_pauseFromClient() throws IOException { //todo -bug
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    System.out.println("client init:");
                    String handshakeAnswer = convo.initExpect(convo.encode("hallo")).nextVariableString();
                    if(! handshakeAnswer.equals("hallo")) fail();

                    System.out.println("client received handshakeAnswer = "+handshakeAnswer+" - client expectAfterPause:");

                    int result = convo.answerExpectAfterPause(convo.encode(1, 5), 10_000).nextInt();
                    System.out.println("client received result, client will close:");
                    convo.close();
                    if(result != 6) fail();
                },
                (convo, m0) -> {//server
                    String handshake = m0.nextVariableString();
                    System.out.println("server was init, received handshake= "+handshake+" - server will send handshake back:");
                    if(! handshake.equals("hallo"))
                        return;
                    P2LMessage result = convo.answerExpect(convo.encode(handshake));
                    System.out.println("server will pause:");
                    convo.pause();

                    System.out.println("server is calculating ...");
                    int a1 = result.nextInt();
                    int a2 = result.nextInt();

                    int calculated = a1 + a2;

                    sleep(5_000);

                    System.out.println("server will answerClose:");
                    convo.answerClose(convo.encode(calculated));
                    System.out.println("server is done");
                });
    }

    @Test void convTest_pauseFromServer() throws IOException {
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode(1));
                    convo.pause();

                    int a1 = m1.nextInt();
                    int a2 = m1.nextInt();

                    int calculated = a1 + a2;
                    sleep(5_000);

                    convo.answerClose(convo.encode(calculated));
                },
                (convo, m0) -> {//server
                    int result = convo.answerExpectAfterPause(convo.encode(m0.nextInt()+1, 4), 10_000).nextInt();
                    if(result != 6) fail();

                    convo.close();
                });
    }

    @Test void convTest_pauseFromClientContinue2mPingPongAfter() throws IOException {
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode("sup?"));

                    P2LMessage m3 = convo.answerExpectAfterPause(convo.encode(m1.nextInt()+2, 5), 10_000);
                    int result = m3.nextInt();
                    if(result != 6) fail();

                    P2LMessage m4 = convo.answerExpect(convo.encode(result+3));
                    boolean success = m4.nextBool();
                    convo.close(); //NOTE: send close regardless of subsequent 'fail' call. Server already knows that it was no success

                    if(!success) fail();
                },
                (convo, m0) -> {//server
                    P2LMessage m2 = convo.answerExpect(convo.encode(-1));

                    int a1 = m2.nextInt();
                    int a2 = m2.nextInt();

                    convo.pause();

                    int calculated = a1 + a2;
                    sleep(5_000);

                    P2LMessage m3 = convo.answerExpect(convo.encode(calculated));
                    int result_p3 = m3.nextInt();
                    boolean success = result_p3 == calculated+3;
                    convo.answerClose(convo.encode(success));
                });
    }

    @Test void convTest_initPauseFromClientContinue2mPingPongAfter() throws IOException {
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpectAfterPause(convo.encode(1, 5), 10_000);
                    int result = m1.nextInt();
                    if(result != 6) fail();

                    P2LMessage m4 = convo.answerExpect(convo.encode(result+3));
                    boolean success = m4.nextBool();
                    convo.close(); //NOTE: send close regardless of subsequent 'fail' call. Server already knows that it was no success

                    if(!success) fail();
                },
                (convo, m0) -> {//server
                    int a1 = m0.nextInt();
                    int a2 = m0.nextInt();

                    convo.pause();

                    int calculated = a1 + a2;
                    sleep(5_000);

                    P2LMessage m3 = convo.answerExpect(convo.encode(calculated));
                    int result_p3 = m3.nextInt();
                    boolean success = result_p3 == calculated+3;
                    convo.answerClose(convo.encode(success));
                });
    }
    @Test void convTest_pauseFromServerContinue2mPingPongAfter() throws IOException {
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode(1));
                    convo.pause();

                    int a1 = m1.nextInt();
                    int a2 = m1.nextInt();

                    int calculated = a1 + a2;
                    sleep(5_000);

                    P2LMessage m3 = convo.answerExpect(convo.encode(calculated));
                    int result_p3 = m3.nextInt();
                    boolean success = result_p3 == calculated+3;
                    convo.answerClose(convo.encode(success));
                },
                (convo, m0) -> {//server
                    P2LMessage m2 = convo.answerExpectAfterPause(convo.encode(m0.nextInt()+1, 4), 10_000);
                    int result = m2.nextInt();
                    if(result != 6) fail();

                    P2LMessage m4 = convo.answerExpect(convo.encode(result+3));
                    boolean success = m4.nextBool();
                    convo.close(); //NOTE: send close regardless of subsequent 'fail' call. Client already knows that it was no success

                    if(!success) fail();
                });
    }






    private static void testEnvConversation(boolean connect, int dropPercentage, int attempts, boolean resetDebug, boolean firstServerAnswerIsCloseOrCloseWith,
                                            ConversationAnswererChangeThisName clientLogic, ConversationAnswererChangeThisName serverLogic) throws IOException {
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
            P2LFuture<Boolean> serverCompletedMultipleTimes = new P2LFuture<>();

            /*m0*/
            nodes[1].registerConversationFor(25, (convo, m0) -> {
                convo.setMaxAttempts(attempts);
                serverLogic.converse(convo, m0);
                if(!firstServerAnswerIsCloseOrCloseWith && bool.isCompleted())
                    serverCompletedMultipleTimes.setCompleted(true);
                bool.setCompleted(true);
            });

            TimeDiffMarker.setMark("convo");
            P2LConversation convo = nodes[0].convo(25, nodes[1].getSelfLink());
            convo.setMaxAttempts(attempts);
            clientLogic.converse(convo, null);

            TimeDiffMarker.println("convo");
            
            assertTrue(bool.get(20_000));
            assertFalse(serverCompletedMultipleTimes.isCompleted());

            TimeDiffMarker.println("convo");

        } finally {
            DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
            DebugStats.MSG_PRINTS_ACTIVE = false;
            DebugStats.print();
            IntermediateTests.close(nodes);
            if(resetDebug) DebugStats.reset();
        }
    }
}
