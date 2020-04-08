import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.conversation.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    @Test void convTest_initExpectClose() throws IOException {
        byte[][] rm = rand(2, 1000);
        testEnvConversation(true, 60, 100, false, true,
                (convo, no) -> {//client
                    ReceivedP2LMessage m1 = convo.initExpectClose(rm[0]);
                    assertArrayEquals(rm[1], m1.asBytes());
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.closeWith(rm[1]);
                });
    }

    @Test void convTest_initCloseAsync() throws IOException {
        byte[][] rm = rand(1, 1000);
        P2LFuture<Boolean> success = new P2LFuture<>();
        testEnvConversation(true, 60, 100, false, false,
                (convo, no) -> {//client
                    convo.initCloseAsync(convo.encodeSingle(rm[0])).callMeBack(success::setCompletedOrCanceledBasedOn);
                    assertTrue(success.get(3000));
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.close();
                });
    }
    @Test void convTest_initExpectCloseAsync() throws IOException {
        byte[][] rm = rand(2, 1000);
        P2LFuture<Boolean> success = new P2LFuture<>();
        testEnvConversation(true, 60, 100, false, true,
                (convo, no) -> {//client
                    convo.initExpectCloseAsync(convo.encodeSingle(rm[0])).callMeBack(m1 -> {
                        assertArrayEquals(rm[1], m1.asBytes());
                        success.setCompleted(true);
                    });
                    assertTrue(success.get(3000));
                },
                (convo, m0) -> {//server
                    assertArrayEquals(rm[0], m0.asBytes());
                    convo.closeWith(rm[1]);
                });
    }


    @Test void convTest_pauseFromClient() throws IOException { //todo -bug
        testEnvConversation(true, 60, 100, false, false,
                (convo, no) -> {//client
                    System.out.println("client init:");
                    String handshakeAnswer = convo.initExpect(convo.encode("hallo")).nextVariableString();
                    if(! handshakeAnswer.equals("hallo")) fail();

                    System.out.println("client received handshakeAnswer = "+handshakeAnswer+" - client expectAfterPause:");

                    int result = convo.answerExpectAfterPause(convo.encode(1, 5), 20_000).nextInt();
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
                    P2LMessage m1 = convo.initExpect(convo.encode(0));
                    convo.pause();

                    int a1 = m1.nextInt();
                    int a2 = m1.nextInt();

                    int calculated = a1 + a2;
                    sleep(5_000);

                    convo.answerClose(convo.encode(calculated));
                },
                (convo, m0) -> {//server
                    int m2_result = convo.answerExpectAfterPause(convo.encode(m0.nextInt()+2, 4), 20_000).nextInt();
                    if(m2_result != 6) fail();

                    convo.close();
                });
    }

    @Test void convTest_pauseFromClientContinue2mPingPongAfter() throws IOException {
        testEnvConversation(true, 60, 100, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode("sup?"));

                    P2LMessage m3 = convo.answerExpectAfterPause(convo.encode(m1.nextInt()+2, 5), 20_000);
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
                        convo.answerExpectAsyncAfterPause(convo.encode(m1.nextInt()+2, 5)).callMeBack(m3 -> {
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
                    P2LMessage m1 = convo.initExpectAfterPause(convo.encode(1, 5), 30_000);
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
                    P2LMessage m2 = convo.answerExpectAfterPause(convo.encode(m0.nextInt()+1, 4), 20_000);
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
            nodes[0].establishConnection(nodes[1].getSelfLink());

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
                        int m2 = msg2.nextInt();
                        convo.tryClose();
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
                for(P2LFuture<?> futi : intermediateFutures) futi.tryCancel(); //this is required that in case of test failure the next test does not also fail - all active retryers are shutdown.
                DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
                DebugStats.MSG_PRINTS_ACTIVE = false;
                IntermediateTests.close(nodes);
                sleep(1000); //ensure nodes close messages to the other node arrive so they don't suddenly close during the next test - ALSO: ensure that the futi cancelation take effect and there are no active retries.
            }
        }
    }



    @Test void convTest_longMessage() throws IOException {
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode(0));

                    ByteArrayStorage m3 = new ByteArrayStorage();
                    convo.answerExpectLong(convo.encode(2), m3, 10000);

                    System.out.println("m3 size = "+m3.contentSize());

                    P2LMessage m5 = convo.answerExpect(convo.encode(4));
                    System.out.println("m5 = " + m5);
                    convo.close();
                },
                (convo, m0) -> {//server
                    P2LMessage m2 = convo.answerExpect(convo.encode(1));

                    ByteArrayStorage m3 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m3.append(rand(1000));
                    P2LMessage m4 = convo.longAnswerExpect(m3, 10000);
                    System.out.println("m4 = " + m4);

                    convo.answerClose(convo.encode(5));
                });
    }

    @Test void convTest_longMessagesInSuccession() throws IOException {
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    P2LMessage m1 = convo.initExpect(convo.encode(0));

                    ByteArrayStorage m3 = new ByteArrayStorage();
                    convo.answerExpectLong(convo.encode(2), m3, 10000);

                    ByteArrayStorage m5 = new ByteArrayStorage();
                    convo.answerExpectLong(convo.encode(4), m5, 10000);

                    convo.answerClose(convo.encode(6));
                },
                (convo, m0) -> {//server
                    P2LMessage m2 = convo.answerExpect(convo.encode(1));

                    ByteArrayStorage m3 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m3.append(rand(1000));
                    P2LMessage m4 = convo.longAnswerExpect(m3, 10000);
                    System.out.println("m4 = " + m4);

                    ByteArrayStorage m5 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m5.append(rand(1000));
                    P2LMessage m6 = convo.longAnswerExpect(m5, 10000);
                    System.out.println("m6 = " + m6);

                    convo.close();
                });
    }

    @Test void convTest_answerLongMessageWithLongMessage() throws IOException {
        throw new UnsupportedOperationException("not currently operational - due to missing bi directional fragment stream");
    }
    @Test void convTest_initWithLongMessage() throws IOException {
        testEnvConversation(true, 0, 1, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLong(convo.encode(0), m1, 10000);

                    convo.answerClose(convo.encode(2));
                },
                (convo, m0) -> {//server
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m1.append(rand(1000));
                    P2LMessage m2 = convo.longAnswerExpect(m1, 10000);
                    System.out.println("m2 = " + m2);

                    convo.close();
                });
    }
    @Test void convTest_closeWithLongMessage() throws IOException {
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLong(convo.encode(0), m1, 10000);
                    convo.close();
                },
                (convo, m0) -> {//server
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m1.append(rand(1000));
                    convo.longAnswerClose(m1, 10000);
                });
    }
    @Test void convTest_longMessageWithPause_closeAfter() throws IOException {
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLongAfterPause(convo.encode(0), m1, 10000);
                    convo.close();
                },
                (convo, m0) -> {//server
                    convo.pause();
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m1.append(rand(1000));

                    sleep(4500);

                    convo.longAnswerClose(m1, 10000);
                });
    }
    @Test void convTest_longMessageWithPause_pingPongBeforeAndAfter() throws IOException {
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLong(convo.encode(0), m1, 10000);
                    ByteArrayStorage m3 = new ByteArrayStorage();
                    convo.answerExpectLongAfterPause(convo.encode(0), m3, 10000);
                    P2LMessage m5 = convo.answerExpect(convo.encode(4));
                    System.out.println("m5 = " + m5);
                    convo.close();
                },
                (convo, m0) -> {//server
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++) m1.append(rand(1000));
                    convo.longAnswerExpect(m1, 10000);

                    convo.pause();

                    sleep(4500);
                    ByteArrayStorage m3 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m3.append(rand(1000));

                    P2LMessage m4 = convo.longAnswerExpect(m3, 10000);
                    System.out.println("m4 = " + m4);

                    convo.answerClose(convo.encode(5));
                });
    }


    @Test
    void convTest_m0_m1L_m2L_close_ASYNC_LONG() throws IOException {
        P2LFuture<Boolean> clientDone = new P2LFuture<>();
        P2LFuture<Boolean> serverDone = new P2LFuture<>();
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLongAsync(convo.encode("sup?"), m1).callMeBack(success -> {
                        if(success==null || !success) fail();
                        assertEquals(1_000_000, m1.contentSize());

                        ByteArrayStorage m2 = new ByteArrayStorage();
                        for(int i=0;i<1000;i++) m2.append(rand(1000));
                        convo.longAnswerCloseAsync(m2).callMeBack(clientDone::setCompleted);
                    });

                    assertTrue(clientDone.get(10000));
                    assertTrue(serverDone.get(10000));
                    System.out.println("clientDone: "+clientDone.getResult());
                    System.out.println("serverDone: "+serverDone.getResult());
                },
                (convo, m0) -> {//server
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++) m1.append(rand(1000));
                    ByteArrayStorage m2 = new ByteArrayStorage();
                    convo.longAnswerExpectLongAsync(m1, m2).callMeBack(success -> {
                        if(success==null || !success) fail();
                        assertEquals(1_000_000, m1.contentSize());
                        convo.tryClose();
                        serverDone.setCompleted(true);
                    });
                });
    }

    @Test
    void convTest_m0_m1L_m2_m3L_close_ASYNC_LONG() throws IOException {
        P2LFuture<Boolean> clientDone = new P2LFuture<>();
        P2LFuture<Boolean> serverDone = new P2LFuture<>();
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLongAsync(convo.encode("m0"), m1).callMeBack(success -> {
                        if(success==null || !success) fail();
                        assertEquals(1_000_000, m1.contentSize());

                        ByteArrayStorage m3 = new ByteArrayStorage();
                        convo.answerExpectLongAsync(convo.encode("m2"), m3).callMeBack(success2 -> {
                            if(success2==null || !success2) fail();
                            assertEquals(500_000, m3.contentSize());
                            convo.tryClose();
                            clientDone.setCompleted(true);
                        });
                    });

                    assertTrue(clientDone.get(10000));
                    assertTrue(serverDone.get(10000));
                    System.out.println("clientDone: "+clientDone.getResult());
                    System.out.println("serverDone: "+serverDone.getResult());
                },
                (convo, m0) -> {//server
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++) m1.append(rand(1000));
                    convo.longAnswerExpectAsync(m1).callMeBack(m2 -> {
                        assertEquals(1_000_000, m1.contentSize());
                        ByteArrayStorage m3 = new ByteArrayStorage();
                        for(int i=0;i<500;i++) m3.append(rand(1000));
                        convo.longAnswerCloseAsync(m3).callMeBack(serverDone::setCompleted);
                    });
                });
    }

    @Test void convTest_longMessageWithPause_closeAfter_ASYNC() throws IOException {
        P2LFuture<Boolean> clientDone = new P2LFuture<>();
        P2LFuture<Boolean> serverDone = new P2LFuture<>();
        testEnvConversation(true, 5, 10, false, false,
                (convo, no) -> {//client
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    convo.initExpectLongAsyncAfterPause(convo.encode(0), m1).callMeBack(success -> {
                        if(success == null || !success) fail();
                        convo.tryClose();
                        clientDone.setCompleted(true);
                    });

                    assertTrue(clientDone.get(10000));
                    assertTrue(serverDone.get(10000));
                    System.out.println("clientDone: "+clientDone.getResult());
                    System.out.println("serverDone: "+serverDone.getResult());
                },
                (convo, m0) -> {//server
                    convo.pause();
                    ByteArrayStorage m1 = new ByteArrayStorage();
                    for(int i=0;i<1000;i++)
                        m1.append(rand(1000));

                    sleep(4500);

                    convo.longAnswerCloseAsync(m1).callMeBack(success -> {
                        if(success == null || !success) fail();
                        serverDone.setCompleted(true);
                    });
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
                try {
                    serverLogic.converse(convo, m0);
                    if (!firstServerAnswerIsCloseOrCloseWith && bool.isCompleted())
                        serverCompletedMultipleTimes.setCompleted(true);
                    bool.trySetCompleted(true);
                } catch (Exception e) {e.printStackTrace();bool.cancel();}
            });

            TimeDiffMarker.setMark("convo");
            P2LConversation convo = nodes[0].convo(25, nodes[1].getSelfLink());
            convo.setMaxAttempts(attempts);
            clientLogic.converse(convo, null);

            TimeDiffMarker.println("convo");
            
            assertTrue(bool.get(50_000));
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
