import jokrey.utilities.debug_analysis_helper.AverageCallTimeMarker;
import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.core.IncomingHandler;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.core.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.core.stream.P2LInputStream;
import jokrey.utilities.network.link2peer.core.stream.P2LOutputStream;
import jokrey.utilities.network.link2peer.util.*;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static jokrey.utilities.network.link2peer.core.IncomingHandler.NUMBER_OF_STREAM_PARTS_RECEIVED;
import static jokrey.utilities.network.link2peer.core.IncomingHandler.NUMBER_OF_STREAM_RECEIPTS_RECEIVED;
import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.rand;
import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

class IntermediateTests {
    @Test void p2lFutureTest_get() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            sleep(100);
            p2lF.setCompleted(123);
            System.out.println("t2("+System.currentTimeMillis()+") completed");
        }).start();
        int x = p2lF.get(500);
        System.out.println("t1("+System.currentTimeMillis()+") - x = "+x);
        assertEquals(123, x);
    }
    @Test void p2lFutureTest_MULTIPLE_GET() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        p2lF.callMeBack(x -> System.out.println("t1 callback: x="+x));
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get(100);
            assertEquals(123, x);
            System.out.println("t2("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t3("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get(100);
            assertEquals(123, x);
            System.out.println("t3("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t4("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get(100);
            assertEquals(123, x);
            System.out.println("t4("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t5("+System.currentTimeMillis()+") init/waiting");
            p2lF.callMeBack(x -> System.out.println("t5 callback: x="+x));
            System.out.println("t5("+System.currentTimeMillis()+") completed");
        }).start();
        new Thread(() -> {
            System.out.println("t6("+System.currentTimeMillis()+") init/waiting");
            p2lF.callMeBack(x -> System.out.println("t6 callback: x="+x));
            System.out.println("t6("+System.currentTimeMillis()+") completed");
        }).start();

        p2lF.setCompleted(123);
        int x = p2lF.get(1);
        assertEquals(123, x);
        System.out.println("t1("+System.currentTimeMillis()+") - x = "+x);
        sleep(1000);
    }
    @Test void p2lFutureTest_callMeBack() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            sleep(100);
            p2lF.setCompleted(123);
            System.out.println("t2("+System.currentTimeMillis()+") completed");
        }).start();

        p2lF.callMeBack(x -> System.out.println("t1("+System.currentTimeMillis()+") - x = "+x));

        sleep(250); // without this the thread is killed automatically when this method returns
        System.out.println("t1 completed");
    }
    @Test void p2lFutureTest_timeout() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            sleep(1000);
            p2lF.setCompleted(123);
            System.out.println("t2("+System.currentTimeMillis()+") completed");
        }).start();

        System.out.println("t1("+System.currentTimeMillis()+") starting wait");
        assertThrows(TimeoutException.class, () -> p2lF.get(100));
        System.out.println("t1("+System.currentTimeMillis()+") completed");
    }
    @Test void p2lFutureTest_combine() {
        P2LFuture<Integer> p2lF1 = new P2LFuture<>();
        P2LFuture<Integer> p2lF2 = new P2LFuture<>();
        P2LFuture<Integer> combine = p2lF1.combine(p2lF2, P2LFuture.PLUS);
        new Thread(() -> {
            p2lF1.setCompleted(1);
            sleep(1000);
            p2lF2.setCompleted(3);
        }).start();

        assertThrows(TimeoutException.class, () -> combine.get(100));
        assertEquals(new Integer(1), p2lF1.get(500));
        assertEquals(new Integer(3), p2lF2.get(1500));
        assertEquals(new Integer(4), combine.get(1500));
    }
    @Test void p2lFutureTest_cancelCombine() {
        P2LFuture<Integer> p2lF1 = new P2LFuture<>();
        P2LFuture<Integer> p2lF2 = new P2LFuture<>();
        P2LFuture<Integer> combine = p2lF1.combine(p2lF2, P2LFuture.PLUS);
        new Thread(() -> {
            p2lF1.setCompleted(1);
            sleep(1000);
            p2lF2.setCompleted(3);
        }).start();

        assertThrows(TimeoutException.class, () -> combine.get(100));
        assertEquals(new Integer(1), p2lF1.get(500));
        assertThrows(AlreadyCompletedException.class, p2lF1::cancel);
        p2lF2.cancel();
        assertThrows(CanceledException.class, combine::get);
        assertThrows(CanceledException.class, p2lF2::get);
    }
    @Test void p2lFutureTest_cannotCompleteTwice() {
        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> p2lF.setCompleted(1)).start();
        sleep(500);

        assertEquals(new Integer(1), p2lF.get(500));
        assertThrows(AlreadyCompletedException.class, () -> p2lF.setCompleted(3));
        assertEquals(new Integer(1), p2lF.get(500));
    }
    @Test void p2lFutureTest_reducePerformanceComparison() {
        int iterations = 1000000;

        reduceUsingCombine(iterations);
        reduceUsingWhenCompleted(iterations);

        for(int repeatCounter=0;repeatCounter<33;repeatCounter++) {
            AverageCallTimeMarker.mark_call_start("reduce using combine");
            reduceUsingCombine(iterations);
            AverageCallTimeMarker.mark_call_end("reduce using combine");

            AverageCallTimeMarker.mark_call_start("reduce using when completed");
            reduceUsingWhenCompleted(iterations);
            AverageCallTimeMarker.mark_call_end("reduce using when completed");
        }
        AverageCallTimeMarker.print_all();
        AverageCallTimeMarker.clear();
    }
    private static void reduceUsingCombine(int iterations) {
        List<P2LFuture<Integer>> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++)
            list.add(new P2LFuture<>(new Integer(1)));

        P2LFuture<Integer> reduced = P2LFuture.reduce(list, P2LFuture.PLUS);
        assertEquals(iterations, reduced.get().intValue());
    }
    private static void reduceUsingWhenCompleted(int iterations) {
        List<P2LFuture<Integer>> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++)
            list.add(new P2LFuture<>(new Integer(1)));

        P2LFuture<Integer> reduced = P2LFuture.reduceWhenCompleted(list, P2LFuture.PLUS);
        assertEquals(iterations, reduced.get().intValue());
    }


    @Test void p2lThreadPoolTest() {
        TimeDiffMarker.setMark(5331);
        {
            ArrayList<P2LFuture<Boolean>> allFs = new ArrayList<>(100);
            P2LThreadPool pool = new P2LThreadPool(2, 32);
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < 100; i++)
                allFs.add(pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                }));
            System.out.println("x1");
            assertTrue(P2LFuture.reduce(allFs, P2LFuture.AND).get());
            System.out.println("x2");
            assertEquals(counter.get(), 100);
            pool.shutdown();
        }
        TimeDiffMarker.println(5331, "custom 1 took: ");


        TimeDiffMarker.setMark(5331);
        {
            P2LThreadPool pool = new P2LThreadPool(2, 32);
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < 100; i++)
                pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                });
            while (counter.get() < 100) sleep(10);
            assertEquals(counter.get(), 100);
            pool.shutdown();
        }
        TimeDiffMarker.println(5331, "custom 2 took: ");

        TimeDiffMarker.setMark(5331);
        {
            ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 32, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < 100; i++)
                pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                });
            while (counter.get() < 100) sleep(10);
            assertEquals(counter.get(), 100);
        }
        TimeDiffMarker.println(5331, "custom took: ");
    }
    @Test void p2lThreadPoolTest_capacity() {
        P2LThreadPool pool = new P2LThreadPool(1, 1, 1);
        AtomicInteger counter = new AtomicInteger(1);
        pool.execute(() -> {
            sleep(200);
            counter.getAndIncrement();
        });
        pool.execute(() -> {
            sleep(200);
            counter.getAndIncrement();
        });
        assertThrows(CapacityReachedException.class, () -> pool.execute(() -> {
            sleep(200);
            counter.getAndIncrement();
        }));
        while (counter.get() < 2) sleep(10);
        assertEquals(counter.get(), 2);
    }



    @Test void establishConnectionProtocolTest() throws IOException {
        P2LNode node1 = P2LNode.create(53189); //creates server thread
        P2LNode node2 = P2LNode.create(53188); //creates server thread

        boolean connected = node2.establishConnection(local(node1)).get(1000);
        assertTrue(connected);

        assertTrue(node2.isConnectedTo(local(node1)));
        assertTrue(node1.isConnectedTo(local(node2)));

        close(node1, node2);
    }



    @Test void garnerConnectionProtocolTest() throws IOException {
        {
            P2LNode[] nodes = generateNodes(10, 60300);

            SocketAddress[] subLinks = new InetSocketAddress[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = local(nodes[i + 2]);
            Set<SocketAddress> successes = nodes[1].establishConnections(subLinks).get(1000);
            for (SocketAddress toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            List<SocketAddress> newConnections = nodes[0].recursiveGarnerConnections(4, local(nodes[1]));

            printPeers(nodes);

            assertEquals(4, newConnections.size());

            assertEquals(4, nodes[0].getEstablishedConnections().size());

            close(nodes);
        }


        {
            P2LNode[] nodes = generateNodes(5, 17681);

            SocketAddress[] subLinks = new InetSocketAddress[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = local(nodes[i + 2]);
            Set<SocketAddress> successes = nodes[1].establishConnections(subLinks).get(1000);
            for (SocketAddress toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            printPeers(nodes);

            List<SocketAddress> newConnections = nodes[0].recursiveGarnerConnections(4, local(nodes[1]));
            assertEquals(4, newConnections.size());

            printPeers(nodes);

            assertEquals(4, nodes[0].getEstablishedConnections().size());
            assertEquals(4, nodes[1].getEstablishedConnections().size());
            assertEquals(2, nodes[2].getEstablishedConnections().size());
            assertEquals(2, nodes[3].getEstablishedConnections().size());
            assertEquals(2, nodes[4].getEstablishedConnections().size());

            close(nodes);
        }

        {
            P2LNode[] nodes = generateNodes(5, 17651);

            SocketAddress[] subLinks = new InetSocketAddress[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = local(nodes[i + 2]);
            Set<SocketAddress> successes = nodes[1].establishConnections(subLinks).get(1000);
            for (SocketAddress toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            List<SocketAddress> newConnections = nodes[0].recursiveGarnerConnections(1000, local(nodes[1]));
            assertEquals(4, newConnections.size());

            printPeers(nodes);

            assertEquals(4, nodes[0].getEstablishedConnections().size());
            assertEquals(4, nodes[1].getEstablishedConnections().size());
            assertEquals(2, nodes[2].getEstablishedConnections().size());
            assertEquals(2, nodes[3].getEstablishedConnections().size());
            assertEquals(2, nodes[4].getEstablishedConnections().size());

            close(nodes);
        }
    }



    @Test void individualMessageTest() throws IOException {
        Map<SocketAddress, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        byte[] idvMsgToSend = new byte[] {17,32,37,45,5,99,33,55,16,43,127};

        int p1 = 54189;
        int p2 = 54188;
        SocketAddress l1 = new InetSocketAddress("localhost", p1);
        SocketAddress l2 = new InetSocketAddress("localhost", p2);
        P2LNode node1 = P2LNode.create(p1); //creates server thread
        node1.addMessageListener(message -> {
            assertEquals(WhoAmIProtocol.toString(l2), message.header.getSender());
            assertArrayEquals(idvMsgToSend, message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(l1, (link, counter) -> counter == null? 1 : counter+1);
        });
        node1.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });
        P2LNode node2 = P2LNode.create(p2); //creates server thread
        node2.addMessageListener(message -> {
            assertEquals(WhoAmIProtocol.toString(l1), message.header.getSender());
            assertArrayEquals(idvMsgToSend, message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(l2, (link, counter) -> counter == null? 1 : counter+1);
        });
        node2.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });

        sleep(100); //let nodes start

        boolean connected = node1.establishConnection(local(node2)).get(1000);
        assertTrue(connected);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.Factory.createSendMessage(0, idvMsgToSend));
        assertTrue(sendResult.get(100));
        sendResult = node1.sendMessageWithReceipt(local(node2), P2LMessage.Factory.createSendMessage(0, idvMsgToSend));
        assertTrue(sendResult.get(100));
//        sendResult = node1.sendMessageWithReceipt(local(node1), P2LMessage.Factory.createSendMessage(0, idvMsgToSend));
//        assertFalse(sendResult.get(100)); //self send does not work - todo: it currently does work to self send messages... it is even possible to be your own peer
        sendResult = node1.sendMessageWithReceipt( new InetSocketAddress("google.com", 123), P2LMessage.Factory.createSendMessage(0, idvMsgToSend));
        assertNull(sendResult.getOrNull(100)); //google is not a connected peer for node 1

        System.out.println("send success");

        sleep(250);

        assertEquals(2, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);

        close(node1, node2);
    }

    private static SocketAddress local(P2LNode node) {
        return new InetSocketAddress("localhost", node.getPort());
    }


    @Test void futureIdvMsgText() throws IOException {
        int p1 = 34189;
        int p2 = 34188;
        P2LNode node1 = P2LNode.create(p1); //creates server thread
        P2LNode node2 = P2LNode.create(p2); //creates server thread

        sleep(100); //let nodes start

//        boolean connected = node1.establishConnection(local(node2)).get(1000);
//        assertTrue(connected);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.Factory.createSendMessage(1, "hallo"));
        assertTrue(sendResult.get(200));
        sendResult = node1.sendMessageWithReceipt(local(node2), P2LMessage.Factory.createSendMessage(1, "welt"));
        assertTrue(sendResult.get(200));

        String node2Received = node2.expectMessage(1).get(200).asString();
        assertEquals("welt", node2Received);
        String node1Received = node1.expectMessage(1).get(200).asString();
        assertEquals("hallo", node1Received);

        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.Factory.createSendMessage(25, "hallo welt!"));
        assertTrue(sendResult.get(200));

        assertThrows(TimeoutException.class, () -> {
            node1.expectMessage(1).get(100); //will timeout, because message was consumed
        });

        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.Factory.createSendMessage(1, "hallo welt"));
        assertTrue(sendResult.get(200));
        String node1Received2 = node1.expectMessage(1).get(100).asString(); //no longer times out, because node 2 has send another message now
        assertEquals("hallo welt", node1Received2);

        assertThrows(TimeoutException.class, () -> {
            node1.expectMessage(local(node1), 25).get(100); //will timeout, because the message is not from node1...
        });
        String node1Received3 = node1.expectMessage(local(node2), 25).get(100).asString();
        assertEquals("hallo welt!", node1Received3);

        close(node1, node2);
    }

    @Test void longMessageTest() throws IOException {
        int p1 = 34191;
        int p2 = 34192;
        P2LNode node1 = P2LNode.create(p1); //creates server thread
        P2LNode node2 = P2LNode.create(p2); //creates server thread

        byte[] toSend_1To2 = new byte[(P2LMessage.CUSTOM_RAW_SIZE_LIMIT - 13) * 2];
        byte[] toSend_2To1 = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT * 20];
        ThreadLocalRandom.current().nextBytes(toSend_1To2);
        ThreadLocalRandom.current().nextBytes(toSend_2To1);
        int randomType = ThreadLocalRandom.current().nextInt(1, 400000);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node1.sendMessageWithReceipt(local(node2), P2LMessage.Factory.createSendMessage(randomType, toSend_1To2));
        assertTrue(sendResult.get(2000));
        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.Factory.createSendMessage(randomType, toSend_2To1));
        assertTrue(sendResult.get(2000));

        P2LMessage message = node1.expectMessage(randomType).get(200);
        assertTrue(message.payloadEquals(toSend_2To1)); //more efficient
        assertArrayEquals(toSend_2To1, message.asBytes());

        message = node2.expectMessage(randomType).get(200);
        assertTrue(message.payloadEquals(toSend_1To2)); //more efficient
        assertArrayEquals(toSend_1To2, message.asBytes());

        close(node1, node2);
    }


    @Test void broadcastMessageTest() throws IOException {
        Map<SocketAddress, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        AtomicReference<byte[]> brdMsgToSend = new AtomicReference<>(new byte[] {17,32,37,45,5,99,33,55,16,43,127});

        int senderPort = 55199;
        SocketAddress senderLink = new InetSocketAddress("localhost", senderPort);

        P2LNode[] nodes = generateNodes(10, 55288, p2Link -> message -> {
            throw new IllegalStateException("this should not be called here");
        }, p2Link -> message -> {
            System.out.println(p2Link + " - IntermediateTests.receivedBroadcastMessage: " + message);
            assertEquals(WhoAmIProtocol.toString(senderLink), message.header.getSender());
            assertArrayEquals(brdMsgToSend.get(), message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(p2Link, (link, counter) -> counter == null? 1 : counter+1);
        });

        P2LNode senderNode = P2LNode.create(senderPort); //creates server thread
        senderNode.addBroadcastListener(message -> {
            throw new IllegalStateException("broadcastReceivedCalledForSender...");
        });
        senderNode.addMessageListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });

        sleep(100); //let nodes start
        P2LFuture<Integer> sendResult;

        printPeers(senderNode);
        printPeers(nodes);
        assertTrue(connectAsLine(nodes).get(1000));
        List<SocketAddress> successLinks = senderNode.recursiveGarnerConnections(4, local(nodes[0]));
        assertEquals(4, successLinks.size());

        System.out.println("successLinks = " + successLinks);
        System.out.println("sending broadcast now");

        TimeDiffMarker.setMark("line + garner 4");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(4, sendResult.get(1000).intValue());

        System.out.println("broadcast send done");

        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }
        TimeDiffMarker.println("line + garner 4");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);


        senderNode.disconnectFromAll();
        for(P2LNode node : nodes) node.disconnectFromAll();
        nodesAndNumberOfReceivedMessages.clear();
        sleep(1000);

        assertTrue(connectAsRing(nodes).get(1000));
        assertTrue(senderNode.establishConnection(local(nodes[0])).get(1000));
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5});
        TimeDiffMarker.setMark("sender + ring");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(1, sendResult.get(1000).intValue());

        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
        }
        TimeDiffMarker.println("sender + ring");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);


        senderNode.disconnectFromAll();
        for(P2LNode node : nodes) node.disconnectFromAll();
        nodesAndNumberOfReceivedMessages.clear();
        sleep(1000);

        assertTrue(connectAsRing(senderNode, nodes).get(1000));
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8});
        TimeDiffMarker.setMark("ring");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(2, sendResult.get(1000).intValue());


        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }
        TimeDiffMarker.println("ring");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);



        senderNode.disconnectFromAll();
        for(P2LNode node : nodes) node.disconnectFromAll();
        nodesAndNumberOfReceivedMessages.clear();
        sleep(1000);

        P2LFuture<Boolean> fullConnectFuture = fullConnect(nodes);
        fullConnectFuture.get(8000);
        sleep(5000);
        senderNode.recursiveGarnerConnections(200, local(nodes[0]), local(nodes[1]));
        System.err.println("FULL CONNECT");
        printPeers(senderNode);
        printPeers(nodes);
        assertTrue(fullConnectFuture.get(1000));

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8,9});

        TimeDiffMarker.setMark("full");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(10, sendResult.get(1000).intValue());


        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }
        TimeDiffMarker.println("full");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);



        senderNode.disconnectFromAll();
        for(P2LNode node : nodes) node.disconnectFromAll();
        nodesAndNumberOfReceivedMessages.clear();
        sleep(1000);

        fullConnectFuture = fullConnect(nodes);
        fullConnectFuture.get(4000);
        System.err.println("FULL CONNECT");
        printPeers(senderNode);
        printPeers(nodes);
        assertTrue(fullConnectFuture.get(1000));
        senderNode.recursiveGarnerConnections(200, local(nodes[0]), local(nodes[1]));
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8,9}); //note that it is the same message as before, the hash nonetheless changes...

        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast(WhoAmIProtocol.toString(senderLink), 10, brdMsgToSend.get())); //IF NOT SUPPLYING A MESSAGE ID, THE OLD MESSAGES WILL BE RECEIVED HERE FIRST....
        assertEquals(10, sendResult.get(1000).intValue());

        for(P2LNode node:nodes) {
            if(new Random().nextBoolean()) {
                assertThrows(TimeoutException.class, () -> node.expectBroadcastMessage(WhoAmIProtocol.toString(local(nodes[0])), 10).get(1000));
                assertArrayEquals(brdMsgToSend.get(), node.expectBroadcastMessage(WhoAmIProtocol.toString(local(senderNode)), 10).get(10000).asBytes());
            } else {
                assertArrayEquals(brdMsgToSend.get(), node.expectBroadcastMessage(10).get(10000).asBytes());
            }
        }
        assertThrows(TimeoutException.class, () -> senderNode.expectBroadcastMessage(10).get(1000)); //sender node will be the only one that does not receive the broadcast (from itself)
        assertThrows(TimeoutException.class, () -> nodes[0].expectBroadcastMessage(10).get(1000)); //also times out, because the message has been consumed by node 0 already


        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);

        close(nodes);
    }

    @Test void messagePassingWithTypeConvertedFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62000);
        ArrayList<P2LFuture<Integer>> fs = new ArrayList<>();
        for(int i=0;i<3;i++) {
            nodes[0].sendMessage(local(nodes[1]), P2LMessage.Factory.createSendMessage(i, new byte[1]));
            fs.add(nodes[1].expectMessage(i).toType(m -> 1));
        }
        Integer result = P2LFuture.reduce(fs, P2LFuture.PLUS).get();
        assertEquals(3, result.intValue());
        close(nodes);
    }
    @Test void messagePassingWithEarlyFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62010);
        P2LFuture<P2LMessage> earlyFuture = nodes[1].expectMessage(1);
        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.Factory.createSendMessage(1, new Integer(142)));
        assertEquals(142, earlyFuture.get(100).nextInt());
        close(nodes);
    }
    @Test void messagePassingWithExpiredFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62020);
        assertThrows(TimeoutException.class, () -> nodes[1].expectMessage(1).get(20)); //expires, and the internal message queue throws it away
        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.Factory.createSendMessage(1, new Integer(142)));
        assertEquals(142, nodes[1].expectMessage(1).get(100).nextInt());
        close(nodes);
    }
    @Test void messagePassingWithCanceledFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62030);
        AtomicReference<P2LFuture<P2LMessage>> future = new AtomicReference<>();
        AtomicInteger successCounter = new AtomicInteger(0);
        new Thread(() -> {
            future.set(nodes[1].expectMessage(1));
            assertThrows(CanceledException.class, ()->future.get().get(500));
            successCounter.getAndIncrement();
        }).start();
        sleep(50);
        future.get().cancel();
//        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.Factory.createSendMessage(1, new Integer(142)));
        sleep(500);//ensure other, canceled future could theoretically receive it, before the next line takes precedence
        assertEquals(142, nodes[1].expectMessage(1).get(100).nextInt());
        assertEquals(1, successCounter.get());
        close(nodes);
    }
    @Test void messagePassingWithCallMeBackFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 60040);
        nodes[1].expectMessage(1).callMeBack(m -> {
            System.out.println("CALLED!!!!!");
            assertEquals(142, m.nextInt());
        });
        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.Factory.createSendMessage(1, new Integer(142)));
        sleep(500); //wait, until the message is actually received by the callback (since the internal message queue prefers the latest registered receiver(STACK))
        assertThrows(TimeoutException.class, () -> nodes[1].expectMessage(1).get(100));
        close(nodes);
    }

    @Test void stillWorksWithDroppedPackagesTest() throws IOException {
        IncomingHandler.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 10;

        P2LNode[] nodes = generateNodes(10, 61408);

//        P2LFuture<Boolean> f = fullConnect(nodes);
//        assertTrue(f.get());
//        printPeers(nodes);


        connectAsRing(nodes);
        nodes[0].establishConnections(local(nodes[2]), local(nodes[4]), local(nodes[5]), local(nodes[7])).waitForIt();
        nodes[2].establishConnections(local(nodes[9]), local(nodes[6]), local(nodes[8]), local(nodes[7])).waitForIt();
        nodes[5].establishConnections(local(nodes[1]), local(nodes[2]), local(nodes[9]), local(nodes[8])).waitForIt();
        nodes[6].establishConnections(local(nodes[0]), local(nodes[2]), local(nodes[9]), local(nodes[7])).waitForIt();
        nodes[8].establishConnections(local(nodes[3])).waitForIt();
        printPeers(nodes);

        for(P2LNode node:nodes)
            node.addBroadcastListener(message -> System.out.println("message = " + message));

        nodes[0].sendMessageBlocking(local(nodes[1]), P2LMessage.Factory.createSendMessage(1, "sup"), 3, 500); //not even an established connection
        nodes[0].sendMessageBlocking(local(nodes[2]), P2LMessage.Factory.createSendMessage(1, "sup"), 3, 500);
        assertEquals("sup", nodes[1].expectMessage(1).get(1).asString());
        assertEquals("sup", nodes[2].expectMessage(1).get(1).asString());
        assertThrows(TimeoutException.class, () -> nodes[3].expectMessage(1).get(1));

        nodes[6].sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast(WhoAmIProtocol.toString(local(nodes[6])), 1, "sup")).waitForIt(20000);

        for(P2LNode node:nodes)
            if(node != nodes[6])
                node.expectBroadcastMessage(1).get(1000);

        close(nodes);

        IncomingHandler.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
    }

    @Test @Disabled void stressTest() {

        //todo do CRAZY STUFF

        //do a simple broadcast test to check whether that still works after all the commotion...
    }



    @Test void streamTest_orderGuaranteed() throws IOException {
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=4;
        P2LNode[] nodes = generateNodes(2, 62820);

        InputStream stream = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";

        new Thread(() -> {
            try {

                int packetCount = P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE-1;
                List<P2LMessage> randomlySplit = toBytesAndSplitRandomly(toSend, packetCount);

                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(local(nodes[0]), m);
                    sleep(500);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(stream, toSend, false);

        close(nodes);
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
    }


    @Test void streamTest_orderNotGuaranteed_guaranteedFewerThanBufferPacketsSend_noDrops() throws IOException {
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
        P2LNode[] nodes = generateNodes(2, 62830);

        InputStream stream = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int packetCount = P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE-1;
                List<P2LMessage> randomlySplit = toBytesAndSplitRandomly(toSend, packetCount);

                for(P2LMessage m:randomlySplit)
                    nodes[1].sendMessage(local(nodes[0]), m);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(stream, toSend, false);

        close(nodes);
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
    }

    @Test void streamTest_sendOrderReverse_guaranteedFewerThanBufferPacketsSend_noDrops() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62840);

        InputStream stream = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int packetCount = P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE-1;
                List<P2LMessage> randomlySplit = toBytesAndSplitRandomly(toSend, packetCount);
                Collections.reverse(randomlySplit);

                for(P2LMessage m:randomlySplit)
                    nodes[1].sendMessage(local(nodes[0]), m);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(stream, toSend, false);

        close(nodes);
    }

    @Test void streamTest_orderGuaranteed_twiceThanBufferSizePacketsSend_noDrops() throws IOException {
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=4;

        P2LNode[] nodes = generateNodes(2, 62850);

        InputStream stream = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {
                int packetCount = P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE*3;
                List<P2LMessage> randomlySplit = toBytesAndSplitRandomly(toSend, packetCount);
//                Collections.reverse(randomlySplit);

                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(local(nodes[0]), m);
                    sleep(500);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(stream, toSend, false);

        close(nodes);

        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
    }

    @Test void streamTest_orderGuaranteedUpUntilBufferSize_twiceThanBufferSizePacketsSend_noDrops() throws IOException {
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=4;
        P2LNode[] nodes = generateNodes(2, 62860);

        InputStream stream = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {

                int packetCount = P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE*3;
                List<P2LMessage> randomlySplit = toBytesAndSplitRandomly(toSend, packetCount);
//                Collections.reverse(randomlySplit);

                int messagesSend = 0;
                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(local(nodes[0]), m);

                    messagesSend++;
                    if(messagesSend%(P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE)==0)
                        sleep(250);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(stream, toSend, false);

        close(nodes);
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
    }

    @Test @Disabled void streamTest_orderNotGuaranteed_twiceThanBufferSizePacketsSend_noDrops() throws IOException {
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=4;
        P2LNode[] nodes = generateNodes(2, 62870);

        InputStream stream = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";
        new Thread(() -> {
            try {

                int packetCount = P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE*3;
                List<P2LMessage> randomlySplit = toBytesAndSplitRandomly(toSend, packetCount);
//                Collections.reverse(randomlySplit);

//                int messagesSend = 0;
                for(P2LMessage m:randomlySplit) {
                    nodes[1].sendMessage(local(nodes[0]), m);

//                    messagesSend++;
//                    if(messagesSend%(P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE)==0)
//                        sleep(250);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(stream, toSend, false);

        close(nodes);
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
    }

    @Test void streamTest_inOut_belowBufferSize() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62880);

        InputStream in = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);
        OutputStream out = nodes[1].getOutputStream(local(nodes[0]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";

        new Thread(() -> {
            try {
                out.write(toSend.getBytes(StandardCharsets.UTF_8));
//                out.flush();//  works also, but close does an internal, direct eof flush
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        streamSplitAssertions(in, toSend, false);

        close(nodes);
    }



    /*
    STREAM_CHUNK_BUFFER_ARRAY_SIZE=4 - 555runs
    =>(11.3s)
        NUMBER_OF_STREAM_RECEIPTS_RECEIVED = 1747
        NUMBER_OF_STREAM_PARTS_RECEIVED = 5250
    STREAM_CHUNK_BUFFER_ARRAY_SIZE=128 - 555runs
    => (5.6s)
        NUMBER_OF_STREAM_RECEIPTS_RECEIVED = 673
        NUMBER_OF_STREAM_PARTS_RECEIVED = 5119
     */
    @Test void streamTest_inOut_twiceBufferSize() throws IOException {
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=4;
        P2LNode[] nodes = generateNodes(2, 62880);

        InputStream in = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);
        OutputStream out = nodes[1].getOutputStream(local(nodes[0]), 1, P2LNode.NO_CONVERSATION_ID);

        String toSend = "hallo\nDies ist ein Test\nDieser String wurde in zufällige Packete aufgespalten und über das stream Protocol gesendet.\nHow do you read?\n";

        P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
            List<P2LMessage> split = toBytesAndSplitRandomly(toSend, 8);
            for(P2LMessage s:split) {
                System.out.println("s.asString() = " + s.asString());
                out.write(s.asBytes());
                out.flush(); //otherwise it is only internally buffered until the buffer is full or it is closed(or flushed like demoed here)
            }
//                out.flush();//  works also, but close does an internal, direct eof flush
            out.close();
            System.out.println("closed");
        });

        streamSplitAssertions(in, toSend, false);

        System.out.println("after assert");
        sendTask.waitForIt();
        System.out.println("after send");

        close(nodes);
        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;

        System.out.println("NUMBER_OF_STREAM_RECEIPTS_RECEIVED = " + NUMBER_OF_STREAM_RECEIPTS_RECEIVED);
        System.out.println("NUMBER_OF_STREAM_PARTS_RECEIVED = " + NUMBER_OF_STREAM_PARTS_RECEIVED);
    }


    @Test void streamTest_inOut_largeArray_usageAsIntended() throws IOException {
//        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=4;
        int oldLimit = P2LMessage.CUSTOM_RAW_SIZE_LIMIT;
        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = P2LMessage.MAX_UDP_PACKET_SIZE;
        P2LNode[] nodes = generateNodes(2, 62880);

        InputStream in = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);
        OutputStream out = nodes[1].getOutputStream(local(nodes[0]), 1, P2LNode.NO_CONVERSATION_ID);

        byte[] toSend = new byte[10_000_000];//10mb
        ThreadLocalRandom.current().nextBytes(toSend);

        P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
            out.write(toSend);
            out.close();
        });

        ByteArrayStorage store = new ByteArrayStorage();
        store.set(0, in, toSend.length);

        assertArrayEquals(toSend, store.getContent());

        sendTask.waitForIt();

        close(nodes);

        P2LMessage.CUSTOM_RAW_SIZE_LIMIT = oldLimit;
//        P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE=128;
    }

    @Test void streamTest_closingOutFirst() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62880);

        P2LInputStream in = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);
        P2LOutputStream out = nodes[1].getOutputStream(local(nodes[0]), 1, P2LNode.NO_CONVERSATION_ID);

        P2LFuture<Boolean> sendTask = P2LThreadPool.executeSingle(() -> {
            out.write(new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16});
            out.close();
        });

        byte[] read = new byte[5];
        int numRead = in.read(read);
        sendTask.waitForIt(); //data still available to read after out closed...

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

        close(nodes);
    }

    @Test void streamTest_closingInFirst() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62880);

        P2LInputStream in = nodes[0].getInputStream(local(nodes[1]), 1, P2LNode.NO_CONVERSATION_ID);
        P2LOutputStream out = nodes[1].getOutputStream(local(nodes[0]), 1, P2LNode.NO_CONVERSATION_ID);

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

        sendTask.waitForIt(); //data still available to read after out closed...

        assertThrows(IOException.class, ()-> in.read(read));

        boolean inClosed = in.isClosed();
        boolean outClosed = out.isClosed(); //note that this is despite that fact that out was never closed actively by the sender thread or anyone else.. it received the notification from the in stream
        assertTrue(inClosed);
        assertTrue(outClosed);

        assertThrows(IOException.class, () -> out.write(1));

        close(nodes);
    }

    @Test void streamTest_inOut_largeArray_usageAsIntended_tcp() throws IOException {
        ServerSocket server = new ServerSocket(6003);
        Socket client = new Socket("localhost", 6003);
        Socket serversConnectionToClient = server.accept();

        InputStream in = serversConnectionToClient.getInputStream();
        OutputStream out = client.getOutputStream();

        byte[] toSend = new byte[10_000_000];//10mb
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


    }

    @Test @Disabled void streamWithDroppedPackagesTest() {
        //todo
    }


    private void streamSplitAssertions(InputStream stream, String toSend, boolean forceClose) {
        int index = 0;
        String[] toSendSplit = toSend.split("\n");
        Scanner s = new Scanner(stream);
        while(s.hasNext()) {
            String line = s.nextLine();
            System.out.println("line = " + line);
            assertEquals(toSendSplit[index++], line);
            if(index == toSendSplit.length && forceClose) {
                s.close();
                break;
            }
        }
        assertEquals(toSendSplit.length, index);
    }
    private List<P2LMessage> toBytesAndSplitRandomly(String toSend, int packetCount) {
        ArrayList<P2LMessage> packets = new ArrayList<>(packetCount);
        byte[] send = P2LMessage.trans.transform(toSend);

        int numBytesSend = 0;
        for(int i=0;i<packetCount;i++) {
            P2LMessageHeader h;
            int numBytesThisPacket;
            int remaining = send.length-numBytesSend;
            if(i+1==packetCount) {
                h = new StreamPartHeader(null, 1, P2LNode.NO_CONVERSATION_ID, i, false, true);
                numBytesThisPacket = remaining;
            } else {
                h = new StreamPartHeader(null, 1, P2LNode.NO_CONVERSATION_ID, i, false, false);
                numBytesThisPacket = Math.min(remaining, rand(0, remaining/Math.max(1, (packetCount/4)) +2));
            }
            byte[] thisPacket = Arrays.copyOfRange(send, numBytesSend, numBytesSend+numBytesThisPacket);
            numBytesSend+=numBytesThisPacket;

            packets.add(h.generateMessage(thisPacket));
        }
        return packets;
    }


    private void printPeers(P2LNode... nodes) {
        for (int i = 0; i < nodes.length; i++)
            System.out.println("node " + i + "("+local(nodes[i])+") peers("+nodes[i].getEstablishedConnections().size()+"): " + nodes[i].getEstablishedConnections());
    }

    private static P2LNode[] generateNodes(int size, int startPort) throws IOException {
        return generateNodes(size, startPort, null, null);
    }
    private static P2LNode[] generateNodes(int size, int startPort, Function<SocketAddress, P2LNode.P2LMessageListener> idvListenerCreator, Function<SocketAddress, P2LNode.P2LMessageListener> brdListenerCreator) throws IOException {
        P2LNode[] nodes = new P2LNode[size];
        SocketAddress[] links = new InetSocketAddress[nodes.length];
        for(int i=0;i<nodes.length;i++) {
            int port = startPort + i;
            links[i] = new InetSocketAddress("127.0.0.1", port);
            nodes[i] = P2LNode.create(port);
            if(idvListenerCreator != null)
                nodes[i].addMessageListener(idvListenerCreator.apply(links[i]));
            if(brdListenerCreator != null)
                nodes[i].addBroadcastListener(brdListenerCreator.apply(links[i]));
        }
        return nodes;
    }
    private static P2LFuture<Boolean> connectAsLine(P2LNode... nodes) {
        ArrayList<P2LFuture<Boolean>> connectionFutures = new ArrayList<>(nodes.length);
        for(int i=0; i<nodes.length-1; i++)
            connectionFutures.add(nodes[i].establishConnection(local(nodes[i+1])));
        return P2LFuture.reduce(connectionFutures, P2LFuture.AND);
    }
    private static P2LFuture<Boolean> connectAsRing(P2LNode... nodes) {
        P2LFuture<Boolean> connectedAsLine = connectAsLine(nodes);
        return nodes[0].establishConnection(local(nodes[nodes.length-1])).combine(connectedAsLine, P2LFuture.AND);
    }
    private static P2LFuture<Boolean> connectAsRing(P2LNode node0, P2LNode... nodes) {
        P2LNode[] allNodes = new P2LNode[nodes.length+1];
        allNodes[0] = node0;
        System.arraycopy(nodes, 0, allNodes, 1, nodes.length);
        return connectAsRing(allNodes);
    }
    private static P2LFuture<Boolean> fullConnect(P2LNode... nodes) {
        System.err.println("full connect begin");
        ArrayList<P2LFuture<Boolean>> connectionFutures = new ArrayList<>(nodes.length);
//        for(int i=0; i<nodes.length; i++) {
////        for(int i=nodes.length-1; i>=0; i--) {
//            for (int ii = i + 1; ii < nodes.length; ii++) {
//                System.err.println(nodes[i].getPort() + " -establishTo- " + nodes[ii].getPort());
////                nodes[i].establishConnection(local(nodes[ii]));
//                connectionFutures.add(nodes[i].establishConnection(local(nodes[ii])));
//            }
//        }

        for(int i=0; i<nodes.length; i++) {
//        for(int i=nodes.length-1; i>=0; i--) {
            for (int ii = i + 1; ii < nodes.length; ii++) {
                System.err.println(nodes[i].getPort() + " -establishTo- " + nodes[ii].getPort());
//                nodes[i].establishConnection(local(nodes[ii]));
//                connectionFutures.add(nodes[i].establishConnection(local(nodes[ii])));
                connectionFutures.add(nodes[ii].establishConnection(local(nodes[i])));
            }
        }
        System.err.println("full connect end");
        return P2LFuture.reduce(connectionFutures, P2LFuture.AND);
//        return new P2LFuture<>(true);
    }
    private static void close(P2LNode... nodes) {
        for(P2LNode node:nodes)
            node.close();
    }
}