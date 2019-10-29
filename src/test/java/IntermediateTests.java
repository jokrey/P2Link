import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.core.IncomingHandler;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.util.CanceledException;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.simple.data_structure.pairs.Pair;
import org.junit.jupiter.api.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class IntermediateTests {
    @Test public void p2lFutureTest_get() {
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
    @Test public void p2lFutureTest_MULTIPLE_GET() {
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
    @Test public void p2lFutureTest_callMeBack() {
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
    @Test public void p2lFutureTest_timeout() {
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
    @Test public void p2lFutureTest_combine() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF1 = new P2LFuture<>();
        P2LFuture<Integer> p2lF2 = new P2LFuture<>();
        P2LFuture<Integer> combine = p2lF1.combine(p2lF2, P2LFuture.COMBINE_PLUS);
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



    @Test public void establishConnectionProtocolTest() throws IOException {
        P2LNode node1 = P2LNode.create(53189); //creates server thread
        P2LNode node2 = P2LNode.create(53188); //creates server thread

        boolean connected = node2.establishConnection(local(node1)).get(1000);
        assertTrue(connected);

        assertTrue(node2.isConnectedTo(local(node1)));
        assertTrue(node1.isConnectedTo(local(node2)));
    }



    @Test public void garnerConnectionProtocolTest() throws IOException {
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
        }
    }

    private void printPeers(P2LNode... nodes) {
        for (int i = 0; i < nodes.length; i++)
            System.out.println("node " + i + "("+local(nodes[i])+") peers("+nodes[i].getEstablishedConnections().size()+"): " + nodes[i].getEstablishedConnections());
    }

    private P2LNode[] generateNodes(int size, int startPort) throws IOException {
        return generateNodes(size, startPort, null, null);
    }
    private P2LNode[] generateNodes(int size, int startPort, Function<SocketAddress, P2LNode.P2LMessageListener> idvListenerCreator, Function<SocketAddress, P2LNode.P2LMessageListener> brdListenerCreator) throws IOException {
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


    @Test public void individualMessageTest() throws IOException {
        Map<SocketAddress, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        byte[] idvMsgToSend = new byte[] {17,32,37,45,5,99,33,55,16,43,127};

        int p1 = 54189;
        int p2 = 54188;
        SocketAddress l1 = new InetSocketAddress("localhost", p1);
        SocketAddress l2 = new InetSocketAddress("localhost", p2);
        P2LNode node1 = P2LNode.create(p1); //creates server thread
        node1.addMessageListener(message -> {
            assertEquals(WhoAmIProtocol.toString(l2), message.sender);
            assertArrayEquals(idvMsgToSend, message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(l1, (link, counter) -> counter == null? 1 : counter+1);
        });
        node1.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });
        P2LNode node2 = P2LNode.create(p2); //creates server thread
        node2.addMessageListener(message -> {
            assertEquals(WhoAmIProtocol.toString(l1), message.sender);
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
        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.createSendMessage(0, idvMsgToSend));
        assertTrue(sendResult.get(100));
        sendResult = node1.sendMessageWithReceipt(local(node2), P2LMessage.createSendMessage(0, idvMsgToSend));
        assertTrue(sendResult.get(100));
//        sendResult = node1.sendMessageWithReceipt(local(node1), P2LMessage.createSendMessage(0, idvMsgToSend));
//        assertFalse(sendResult.get(100)); //self send does not work - todo: it currently does work to self send messages... it is even possible to be your own peer
        sendResult = node1.sendMessageWithReceipt( new InetSocketAddress("google.com", 123), P2LMessage.createSendMessage(0, idvMsgToSend));
        assertNull(sendResult.getOrNull(100)); //google is not a connected peer for node 1

        System.out.println("send success");

        sleep(250);

        assertEquals(2, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);
    }

    private static SocketAddress local(P2LNode node) {
        return new InetSocketAddress("localhost", node.getPort());
    }


    @Test public void futureIdvMsgText() throws IOException {
        int p1 = 34189;
        int p2 = 34188;
        SocketAddress l1 = new InetSocketAddress("localhost", p1);
        SocketAddress l2 = new InetSocketAddress("localhost", p2);
        P2LNode node1 = P2LNode.create(p1); //creates server thread
        P2LNode node2 = P2LNode.create(p2); //creates server thread

        sleep(100); //let nodes start

        boolean connected = node1.establishConnection(local(node2)).get(1000);
        assertTrue(connected);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.createSendMessage(1, "hallo"));
        assertTrue(sendResult.get(200));
        sendResult = node1.sendMessageWithReceipt(local(node2), P2LMessage.createSendMessage(1, "welt"));
        assertTrue(sendResult.get(200));

        String node2Received = node2.expectMessage(1).get(200).asString();
        assertEquals("welt", node2Received);
        String node1Received = node1.expectMessage(1).get(200).asString();
        assertEquals("hallo", node1Received);

        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.createSendMessage(25, "hallo welt!"));
        assertTrue(sendResult.get(200));

        assertThrows(TimeoutException.class, () -> {
            node1.expectMessage(1).get(100); //will timeout, because message was consumed
        });

        sendResult = node2.sendMessageWithReceipt(local(node1), P2LMessage.createSendMessage(1, "hallo welt"));
        assertTrue(sendResult.get(200));
        String node1Received2 = node1.expectMessage(1).get(100).asString(); //no longer times out, because node 2 has send another message now
        assertEquals("hallo welt", node1Received2);

        assertThrows(TimeoutException.class, () -> {
            node1.expectMessage(local(node1), 25).get(100).asString(); //will timeout, because the message is not from node1...
        });
        String node1Received3 = node1.expectMessage(local(node2), 25).get(100).asString();
        assertEquals("hallo welt!", node1Received3);
    }


    @Test public void broadcastMessageTest() throws IOException {
        Map<SocketAddress, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        AtomicReference<byte[]> brdMsgToSend = new AtomicReference<>(new byte[] {17,32,37,45,5,99,33,55,16,43,127});

        int senderPort = 55199;
        SocketAddress senderLink = new InetSocketAddress("localhost", senderPort);

        P2LNode[] nodes = generateNodes(10, 55288, p2Link -> message -> {
            throw new IllegalStateException("this should not be called here");
        }, p2Link -> message -> {
            System.out.println(p2Link + " - IntermediateTests.receivedBroadcastMessage: " + message);
            assertEquals(WhoAmIProtocol.toString(senderLink), message.sender);
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
        P2LFuture<Pair<Integer, Integer>> sendResult;

        printPeers(senderNode);
        printPeers(nodes);
        assertTrue(connectAsLine(nodes).get(1000));
        List<SocketAddress> successLinks = senderNode.recursiveGarnerConnections(4, local(nodes[0]));
        assertEquals(4, successLinks.size());

        System.out.println("successLinks = " + successLinks);
        System.out.println("sending broadcast now");

        TimeDiffMarker.setMark("line + garner 4");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(new Pair<>(4, 4), sendResult.get(1000));

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
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(new Pair<>(1, 1), sendResult.get(1000));

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
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(new Pair<>(2, 2), sendResult.get(1000));


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
        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.createBroadcast(WhoAmIProtocol.toString(senderLink), 0, brdMsgToSend.get()));
        assertEquals(new Pair<>(10, 10), sendResult.get(1000));


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

        sendResult = senderNode.sendBroadcastWithReceipts(P2LMessage.createBroadcast(WhoAmIProtocol.toString(senderLink), 10, brdMsgToSend.get())); //IF NOT SUPPLYING A MESSAGE ID, THE OLD MESSAGES WILL BE RECEIVED HERE FIRST....
        assertEquals(new Pair<>(10, 10), sendResult.get(1000));

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
    }

    @Test public void messagePassingWithTypeConvertedFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62000);
        ArrayList<P2LFuture<Integer>> fs = new ArrayList<>();
        for(int i=0;i<3;i++) {
            nodes[0].sendMessage(local(nodes[1]), P2LMessage.createSendMessage(i, new byte[1]));
            fs.add(nodes[1].expectMessage(i).toType(m -> 1));
        }
        Integer result = P2LFuture.combine(fs, P2LFuture.COMBINE_PLUS).get();
        assertEquals(3, result.intValue());
    }
    @Test public void messagePassingWithEarlyFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62010);
        P2LFuture<P2LMessage> earlyFuture = nodes[1].expectMessage(1);
        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.createSendMessage(1, new Integer(142)));
        assertEquals(142, earlyFuture.get(100).nextInt());
    }
    @Test public void messagePassingWithExpiredFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62020);
        assertThrows(TimeoutException.class, () -> nodes[1].expectMessage(1).get(20)); //expires, and the internal message queue throws it away
        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.createSendMessage(1, new Integer(142)));
        assertEquals(142, nodes[1].expectMessage(1).get(100).nextInt());
    }
    @Test public void messagePassingWithCanceledFutureWorks() throws IOException {
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
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.createSendMessage(1, new Integer(142)));
        sleep(500);//ensure other future could receive it, before the next line takes precedence
        assertEquals(142, nodes[1].expectMessage(1).get(100).nextInt());
        assertEquals(1, successCounter.get());
    }
    @Test public void messagePassingWithCallMeBackFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 64000);
        nodes[1].expectMessage(1).callMeBack(m -> {
            System.out.println("CALLED!!!!!");
            assertEquals(142, m.nextInt());
        });
        sleep(500);
        nodes[0].sendMessage(local(nodes[1]), P2LMessage.createSendMessage(1, new Integer(142)));
        sleep(500); //wait, until the message is actually received by the callback (since the internal message queue prefers the latest registered receiver(STACK))
        assertThrows(TimeoutException.class, () -> nodes[1].expectMessage(1).get(100));
    }

    @Test public void stillWorksWithDroppedPackagesTest() throws IOException {
        IncomingHandler.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;

        //TODO: why does this not work, but practically the same scenario now works in broadcast test.....???
//         it is not the port numbers
        P2LNode[] nodes = generateNodes(10, 61408);

//        P2LFuture<Boolean> f = connectAsRing(nodes);
//        f.get(8000);
//        printPeers(nodes);
//        assertTrue(f.get());

        sleep(1000);

        P2LFuture<Boolean> f = fullConnect(nodes);
        Boolean result = f.get();
        printPeers(nodes);
        assertTrue(result);


//        connectAsRing(nodes);
//        Set<SocketAddress> successes = nodes[0].establishConnections(local(nodes[2])).get();
//        System.err.println("successes = " + successes);
//        assertEquals(1, successes.size());
//        nodes[0].establishConnections(local(nodes[2]), local(nodes[4]), local(nodes[5]), local(nodes[7]));
//        nodes[2].establishConnections(local(nodes[9]), local(nodes[6]), local(nodes[8]), local(nodes[7]));
//        nodes[5].establishConnections(local(nodes[1]), local(nodes[2]), local(nodes[9]), local(nodes[8]));
//        nodes[6].establishConnections(local(nodes[0]), local(nodes[2]), local(nodes[9]), local(nodes[7]));
//        nodes[8].establishConnections(local(nodes[3]));
//        printPeers(nodes);
//
//
//        for(P2LNode node:nodes)
//            node.addBroadcastListener(message -> System.out.println("message = " + message));
//
//        nodes[0].sendMessageBlocking(local(nodes[1]), P2LMessage.createSendMessage(1, "sup"), 3, 500); //not even an established connection
//        nodes[0].sendMessageBlocking(local(nodes[2]), P2LMessage.createSendMessage(1, "sup"), 3, 500);
//        assertEquals("sup", nodes[1].expectMessage(1).get(1).asString());
//        assertEquals("sup", nodes[2].expectMessage(1).get(1).asString());
//        assertThrows(TimeoutException.class, () -> nodes[3].expectMessage(1).get(1));
//
//        nodes[6].sendBroadcastWithReceipts(P2LMessage.createBroadcast(WhoAmIProtocol.toString(local(nodes[6])), 1, "sup")).waitForIt();
//
//        for(P2LNode node:nodes)
//            if(node != nodes[6])
//                node.expectBroadcastMessage(1).get(1000);
        IncomingHandler.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
    }

    @Test public void stressTest() {

        //todo do CRAZY STUFF

        //do a simple broadcast test to check whether that still works after all the commotion...

        throw new NotImplementedException();
    }


    private P2LFuture<Boolean> connectAsLine(P2LNode... nodes) {
        ArrayList<P2LFuture<Boolean>> connectionFutures = new ArrayList<>(nodes.length);
        for(int i=0; i<nodes.length-1; i++)
            connectionFutures.add(nodes[i].establishConnection(local(nodes[i+1])));
        return P2LFuture.combine(connectionFutures, P2LFuture.COMBINE_AND);
    }
    private P2LFuture<Boolean> connectAsRing(P2LNode... nodes) {
        P2LFuture<Boolean> connectedAsLine = connectAsLine(nodes);
        return nodes[0].establishConnection(local(nodes[nodes.length-1])).combine(connectedAsLine, P2LFuture.COMBINE_AND);
    }
    private P2LFuture<Boolean> connectAsRing(P2LNode node0, P2LNode[] nodes) {
        P2LNode[] allNodes = new P2LNode[nodes.length+1];
        allNodes[0] = node0;
        System.arraycopy(nodes, 0, allNodes, 1, nodes.length);
        return connectAsRing(allNodes);
    }
    private P2LFuture<Boolean> fullConnect(P2LNode[] nodes) {
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
        return P2LFuture.combine(connectionFutures, P2LFuture.COMBINE_AND);
//        return new P2LFuture<>(true);
    }


    @Test public void p2lThreadPoolTest() {
        TimeDiffMarker.setMark(5331);
        {
            P2LThreadPool pool = new P2LThreadPool(2, 32);
            AtomicInteger counter = new AtomicInteger(1);
            for (int i = 0; i < 100; i++)
                pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                });
            while (counter.get() < 100) sleep(10);
        }
        TimeDiffMarker.println(5331, "custom took: ");

        TimeDiffMarker.setMark(5331);
        {
            ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 32, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));
            AtomicInteger counter = new AtomicInteger(1);
            for (int i = 0; i < 100; i++)
                pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                });
            while (counter.get() < 100) sleep(10);
        }
        TimeDiffMarker.println(5331, "custom took: ");
    }
}