import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.core.P2LConversation;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

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
        testEnvConversation(true, 60, 100, false, false,
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
        testEnvConversation(false, 60, 100, false, false,
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
        testEnvConversation(true, 60, 100, false, false,
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
        testEnvConversation(true, 60, 100, false, false,
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
        testEnvConversation(true, 60, 100, false, true,
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
        testEnvConversation(true, 60, 100, false, true,
                (convo, no) -> {//client
                     convo.initClose(rm[0]);
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.close();
                });
    }


    @Test void convTest_pauseFromClient() throws IOException { //todo -bug
        testEnvConversation(true, 60, 100, false, false,
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
        testEnvConversation(true, 60, 100, false, false,
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
        testEnvConversation(true, 60, 100, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode("sup?"));

                    P2LMessage m3 = convo.answerExpectAfterPause(convo.encode(m1.nextInt()+2, 5), 10_000);
                    int result = m3.nextInt();
                    if(result != 6) fail();

                    P2LMessage m5 = convo.answerExpect(convo.encode(result+3));
                    boolean success = m5.nextBool();
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

                    P2LMessage m4 = convo.answerExpect(convo.encode(calculated));
                    int result_plus3 = m4.nextInt();
                    boolean success = result_plus3 == calculated+3;
                    convo.answerClose(convo.encode(success));
                });
    }

    @Test
    void convTest_pauseFromClientContinue2mPingPongAfter_ASYNC() throws IOException {
        P2LFuture<Boolean> clientDone = new P2LFuture<>();
        P2LFuture<Boolean> serverDone = new P2LFuture<>();
        testEnvConversation(true, 60, 100, false, false,
                (convo, no) -> {//client
                    convo.initExpectAsync(convo.encode("sup?")).callMeBack(m1 -> {
                        convo.answerExpectAsyncAfterPause(convo.encode(m1.nextInt()+2, 5), 10_000).callMeBack(m3 -> {
                            int result = m3.nextInt();
                            if(result != 6) fail();

                            convo.answerExpectAsync(convo.encode(result+3)).callMeBack(m5 -> {
                                boolean success = m5.nextBool();
                                convo.tryClose(); //NOTE: send close regardless of subsequent 'fail' call. Server already knows that it was no success

                                if(!success) fail();
                                clientDone.setCompleted(true);
                            });
                        });
                    });

                    assertTrue(clientDone.get(30000));
                    assertTrue(serverDone.get(30000));
                    System.out.println("clientDone: "+clientDone.getResult());
                    System.out.println("serverDone: "+serverDone.getResult());
                },
                (convo, m0) -> {//server
                    convo.answerExpectAsync(convo.encode(-1)).callMeBack(m2 -> {
                        System.out.println("server - m2 start");
                        int a1 = m2.nextInt();
                        int a2 = m2.nextInt();

                        convo.tryPause();

                        int calculated = a1 + a2;
                        sleep(5_000);

                        System.out.println("server - m2 mid");
                        convo.answerExpectAsync(convo.encode(calculated)).callMeBack(m4 -> {
                            int result_plus3 = m4.nextInt();
                            boolean success = result_plus3 == calculated+3;
                            System.out.println("received m4");
                            convo.answerCloseAsync(convo.encode(success)).callMeBack(condition -> {
                                Assertions.assertTrue(condition);
                                serverDone.setCompleted(true);
                            });
                        });
                        System.out.println("server - m2 end");
                    });
                });
    }

    @Test void convTest_initPauseFromClientContinue2mPingPongAfter() throws IOException {
        testEnvConversation(true, 60, 100, false, false,
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
        testEnvConversation(true, 60, 100, false, false,
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

    @Test
    void convTest_asyncSimpleTest() throws IOException {
        DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
        DebugStats.MSG_PRINTS_ACTIVE = false;
        { //SYNCHRONIZED/THREAD_MAINTAINING EXAMPLE
            P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);
            try {
                P2LFuture<Boolean> fut = new P2LFuture<>();
                nodes[1].registerConversationFor(1, (convo, msg0) -> {
                    int m0 = msg0.nextInt();
                    assertEquals(0, m0);
                    int m2 = convo.answerExpect(convo.encode(1)).nextInt();
                    assertEquals(2, m2);
                    fut.setCompleted(true);
                    convo.close();
                });

                P2LConversation convo = nodes[0].convo(1, nodes[1].getSelfLink());
                int m1 = convo.initExpect(convo.encode(0)).nextInt();
                assertEquals(1, m1);
                convo.answerClose(convo.encode(2));

                assertTrue(fut.get(1)); //VERY SHORT TIMEOUT, because at that time it should already be completed
            } finally {
                IntermediateTests.close(nodes);
            }
        }


        DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 60;
        DebugStats.MSG_PRINTS_ACTIVE = true;

        { //ASYNC/NO_WAIT_THREAD_MAINTAINING EXAMPLE
            P2LNode[] nodes = IntermediateTests.generateNodes(2, 62880);
            List<P2LFuture<?>> intermediateFutures = new ArrayList<>();
            try {
                P2LFuture<Boolean> serverCompletedFut = new P2LFuture<>();
                P2LFuture<Boolean> clientCompletedFut = new P2LFuture<>();

                nodes[1].registerConversationFor(200, (convo, msg0) -> {
                    convo.setMaxAttempts(100);
                    int m0 = msg0.nextInt();
                    assertEquals(0, m0);
                    System.out.println("m0 = " + m0);
                    P2LFuture<?> futi_s1 = convo.answerExpectAsync(convo.encode(1)).callMeBack(msg2 -> {
                        convo.tryClose();
                        int m2 = msg2.nextInt();
                        assertEquals(2, m2);
                        System.out.println("m2 = " + m2);
                        serverCompletedFut.setCompleted(true);
                    });
                    intermediateFutures.add(futi_s1);
                });

                P2LConversation convo = nodes[0].convo(200, nodes[1].getSelfLink());
                convo.setMaxAttempts(100);
                P2LFuture<?> futi_c1 = convo.initExpectAsync(convo.encode(0)).callMeBack(msg1 -> {
                    int m1 = msg1.nextInt();
                    assertEquals(1, m1);
                    System.out.println("m1 = " + m1);
                    P2LFuture<?> futi_c2 = convo.answerCloseAsync(convo.encode(2)).callMeBack(success -> {
                        assertTrue(serverCompletedFut.get(1)); //VERY SHORT TIMEOUT, because at that time it should already be completed
                        if(success!=null) {
                            assertTrue(success);
                            System.out.println("test1");
                            clientCompletedFut.setCompleted(true);
                        }
                    });
                    intermediateFutures.add(futi_c2);
                });
                intermediateFutures.add(futi_c1);

                assertTrue(serverCompletedFut.get(30_000));
                assertTrue(clientCompletedFut.get(30_000));
            } finally {
                for(P2LFuture<?> futi : intermediateFutures) futi.cancelIfNotCompleted(); //this is required that in case of test failure the next test does not also fail - all active retryers are shutdown.
                DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
                DebugStats.MSG_PRINTS_ACTIVE = false;
                IntermediateTests.close(nodes);
                sleep(1000); //ensure nodes close messages to the other node arrive so they don't suddenly close during the next test - ALSO: ensure that the futi cancelation take effect and there are no active retries.
            }
        }
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

            sleep(10); //unlikely: - done in case before close was reached, the other side retried and received an answer - in that case this entire function may hit, the nodes closed but close still wants to attempt sending (only if close on server side naturally)

            IntermediateTests.close(nodes);
            sleep(100); //ensure nodes close messages to the other node arrive so they don't suddenly close during the next test
            if(resetDebug) DebugStats.reset();
        }
    }
}
