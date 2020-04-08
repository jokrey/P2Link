import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.encoder.tag_based.implementation.paired.length_indicator.type.transformer.LITypeToBytesTransformer;
import jokrey.utilities.network.link2peer.*;
import jokrey.utilities.network.link2peer.P2Link.Local;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.NodeCreator;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.MinimalHeader;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.node.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.node.protocols.RelayedConnectionProtocol;
import jokrey.utilities.network.link2peer.util.CanceledException;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static jokrey.utilities.network.link2peer.P2LMessage.MAX_EXPIRATION_TIMEOUT;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_CONVERSATION_ID;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_STEP;
import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.rand;
import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

class IntermediateTests {
    @Test void establishConnectionProtocolTest() throws IOException {
        P2LNode node1 = P2LNode.create(Local.forTest(53189)); //creates server thread
        P2LNode node2 = P2LNode.create(Local.forTest(53188)); //creates server thread
        try {
            boolean connected = node2.establishConnection(node1.getSelfLink()).get(1000);
            assertTrue(connected);

            assertTrue(node2.isConnectedTo(node1.getSelfLink()));
            assertTrue(node1.isConnectedTo(node2.getSelfLink()));
        } finally {
            close(node1, node2);
        }
    }

    @Test void establishConnectionProtocolTest_RELAY() throws IOException {
        P2LNode relayServer = P2LNode.create(Local.forTest(5319)); //creates server thread
        P2LNode node1 = P2LNode.create(Local.forTest(53101)); //creates server thread
        P2LNode node2 = P2LNode.create(Local.forTest(53102)); //creates server thread
        try {
            node1.establishConnection(relayServer.getSelfLink()).waitForIt();

            P2Link node1LinkAsKnownTo2 = ((Local) node1.getSelfLink()).withRelay(((Local) relayServer.getSelfLink()).unsafeAsDirect());

            boolean connected = node2.establishConnection(node1LinkAsKnownTo2).get(2000);
            assertTrue(connected);

            sleep(100);

            assertTrue(node2.isConnectedTo(node1.getSelfLink()));
            assertTrue(node1.isConnectedTo(node2.getSelfLink()));
        } finally {
            close(relayServer, node1, node2);
        }
    }




    @Test void garnerConnectionProtocolTest() throws IOException {
        DebugStats.MSG_PRINTS_ACTIVE = true;
        {
            P2LNode[] nodes = generateNodes(10, 60300);

            P2Link[] subLinks = new P2Link[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = nodes[i + 2].getSelfLink();
            for(P2Link subLink:subLinks) {
                assertTrue(nodes[1].establishConnection(subLink).get(2500));
                sleep(50); //congestion control
            }
//            Set<P2Link> successes = nodes[1].establishConnections(subLinks).get(10000); //should work, but too many packages are dropped on devices with low local bandwidth
//            for (P2Link toBeConnected : subLinks)
//                assertTrue(successes.contains(toBeConnected));

            List<P2Link> newConnections = nodes[0].recursiveGarnerConnections(4, nodes[1].getSelfLink());

            printPeers(nodes);

            assertEquals(4, newConnections.size());

            assertEquals(4, nodes[0].getEstablishedConnections().length);

            close(nodes);
        }


        {
            P2LNode[] nodes = generateNodes(5, 17681);

            P2Link[] subLinks = new P2Link[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = nodes[i + 2].getSelfLink();
            Set<P2Link> successes = nodes[1].establishConnections(subLinks).get(1000);
            for (P2Link toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            printPeers(nodes);

            List<P2Link> newConnections = nodes[0].recursiveGarnerConnections(4, nodes[1].getSelfLink());
            assertEquals(4, newConnections.size());

            printPeers(nodes);

            assertEquals(4, nodes[0].getEstablishedConnections().length);
            assertEquals(4, nodes[1].getEstablishedConnections().length);
            assertEquals(2, nodes[2].getEstablishedConnections().length);
            assertEquals(2, nodes[3].getEstablishedConnections().length);
            assertEquals(2, nodes[4].getEstablishedConnections().length);

            close(nodes);
        }

        {
            P2LNode[] nodes = generateNodes(5, 17651);

            P2Link[] subLinks = new P2Link[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = nodes[i + 2].getSelfLink();
            Set<P2Link> successes = nodes[1].establishConnections(subLinks).get(1000);
            for (P2Link toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            List<P2Link> newConnections = nodes[0].recursiveGarnerConnections(1000, nodes[1].getSelfLink());
            assertEquals(4, newConnections.size());

            printPeers(nodes);

            assertEquals(4, nodes[0].getEstablishedConnections().length);
            assertEquals(4, nodes[1].getEstablishedConnections().length);
            assertEquals(2, nodes[2].getEstablishedConnections().length);
            assertEquals(2, nodes[3].getEstablishedConnections().length);
            assertEquals(2, nodes[4].getEstablishedConnections().length);

            close(nodes);
        }
    }



    @Test void individualMessageTest() throws IOException {
        Map<P2Link, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        byte[] idvMsgToSend = new byte[] {17,32,37,45,5,99,33,55,16,43,127};

        int p1 = 54189;
        int p2 = 54188;
        P2Link.Local l1 = Local.forTest(p1);
        P2Link.Local l2 = Local.forTest( p2);
        P2LNode node1 = P2LNode.create(l1); //creates server thread
        P2LNode node2 = P2LNode.create(l2); //creates server thread

        sleep(100); //let nodes start

        boolean connected = node1.establishConnection(node2.getSelfLink()).get(1000);
        assertTrue(connected);

        P2Link.Direct node1LinkVisibleToNode2 = node1.whoAmI(l2.unsafeAsDirect().resolve()).get(1000);
        P2Link.Direct node2LinkVisibleToNode1 = node2.whoAmI(l1.unsafeAsDirect().resolve()).get(1000);

        node1.addMessageListener(message -> {
            assertEquals(node2LinkVisibleToNode1.resolve(), message.sender);
            assertArrayEquals(idvMsgToSend, message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(l1, (link, counter) -> counter == null? 1 : counter+1);
        });
        node1.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });
        node2.addMessageListener(message -> {
            assertEquals(node1LinkVisibleToNode2.resolve(), message.sender);
            assertArrayEquals(idvMsgToSend, message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(l2, (link, counter) -> counter == null? 1 : counter+1);
        });
        node2.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendMessageWithReceipt(node1.getSelfLink(), P2LMessage.with(0, idvMsgToSend));
        assertTrue(sendResult.get(100));
        sendResult = node1.sendMessageWithReceipt(node2.getSelfLink(), P2LMessage.with(0, idvMsgToSend));
        assertTrue(sendResult.get(100));
//        sendResult = node1.sendMessageWithReceipt(node1.getSelfLink(), P2LMessage.with(0, idvMsgToSend));
//        assertFalse(sendResult.get(100)); //self send does not work - todo: it currently does work to self send messages... it is even possible to be your own peer
        sendResult = node1.sendMessageWithReceipt( new P2Link.Direct("google.com", 123), P2LMessage.with(0, idvMsgToSend));
        assertNull(sendResult.getOrNull(100)); //google is not a connected peer for node 1

        System.out.println("send success");

        sleep(250);

        assertEquals(2, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);

        close(node1, node2);
    }


    @Test void futureIdvMsgText() throws IOException {
        int p1 = 34189;
        int p2 = 34188;
        P2LNode node1 = P2LNode.create(Local.forTest(p1)); //creates server thread
        P2LNode node2 = P2LNode.create(Local.forTest(p2)); //creates server thread

        sleep(100); //let nodes start

//        boolean connected = node1.establishConnection(node2.getSelfLink()).get(1000);
//        assertTrue(connected);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendMessageWithReceipt(node1.getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, "hallo".getBytes(UTF_8)));
        assertTrue(sendResult.get(200));
        sendResult = node1.sendMessageWithReceipt(node2.getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, "welt".getBytes(UTF_8)));
        assertTrue(sendResult.get(200));

        String node2Received = node2.expectMessage(1).get(200).asString();
        assertEquals("welt", node2Received);
        String node1Received = node1.expectMessage(1).get(200).asString();
        assertEquals("hallo", node1Received);

        sendResult = node2.sendMessageWithReceipt(node1.getSelfLink(), P2LMessage.withExpiration(25, MAX_EXPIRATION_TIMEOUT, "hallo welt!".getBytes(UTF_8)));
        assertTrue(sendResult.get(200));

        assertThrows(TimeoutException.class, () -> {
            node1.expectMessage(1).get(100); //will timeout, because message was consumed
        });

        sendResult = node2.sendMessageWithReceipt(node1.getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, "hallo welt".getBytes(UTF_8)));
        assertTrue(sendResult.get(200));
        String node1Received2 = node1.expectMessage(1).get(100).asString(); //no longer times out, because node 2 has send another message now
        assertEquals("hallo welt", node1Received2);

        assertThrows(TimeoutException.class, () -> {
            node1.expectMessage(node1.getSelfLink(), 25).get(100); //will timeout, because the message is not from node1...
        });
        String node1Received3 = node1.expectMessage(node2.getSelfLink(), 25).get(100).asString();
        assertEquals("hallo welt!", node1Received3);

        close(node1, node2);
    }

    @Test void longMessageTest() throws IOException {
        int p1 = 34191;
        int p2 = 34192;
        P2LNode node1 = P2LNode.create(Local.forTest(p1)); //creates server thread
        P2LNode node2 = P2LNode.create(Local.forTest(p2)); //creates server thread

        byte[] toSend_1To2 = new byte[(P2LMessage.CUSTOM_RAW_SIZE_LIMIT - 15) * 2];
        byte[] toSend_2To1 = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT * 2];
        ThreadLocalRandom.current().nextBytes(toSend_1To2);
        ThreadLocalRandom.current().nextBytes(toSend_2To1);
//        int randomType = ThreadLocalRandom.current().nextInt(1, 400000);
        int randomType = 4; //chosen by fair dice roll - not my joker by the way

        printPeers(node1, node2);

        DebugStats.MSG_PRINTS_ACTIVE = true;
//        P2LFuture<Boolean> sendResult;
//        sendResult =
                node1.sendMessageWithRetries(node2.getSelfLink(), P2LMessage.withExpiration(randomType, MAX_EXPIRATION_TIMEOUT, toSend_1To2), 3, 500);
//        assertTrue(sendResult.get(2000));
//        sendResult =
                node2.sendMessageWithRetries(node1.getSelfLink(), P2LMessage.withExpiration(randomType, MAX_EXPIRATION_TIMEOUT, toSend_2To1), 3, 500);
//        assertTrue(sendResult.get(2000));
        DebugStats.MSG_PRINTS_ACTIVE = false;

        P2LMessage message = node1.expectMessage(randomType).get(200);
        assertTrue(message.payloadEquals(toSend_2To1)); //more efficient
        assertArrayEquals(toSend_2To1, message.asBytes());

        message = node2.expectMessage(randomType).get(200);
        assertTrue(message.payloadEquals(toSend_1To2)); //more efficient
        assertArrayEquals(toSend_1To2, message.asBytes());

        close(node1, node2);
    }


    @Test void broadcastMessageTest() throws IOException {
        Map<P2Link, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        AtomicReference<byte[]> brdMsgToSend = new AtomicReference<>(new byte[] {17,32,37,45,5,99,33,55,16,43,127});

        int senderPort = 55199;
        P2Link senderLink = Local.forTest(senderPort);

        P2LNode[] nodes = generateNodes(10, 55288, p2Link -> message -> {
            throw new IllegalStateException("this should not be called here");
        }, p2Link -> message -> {
            System.out.println(p2Link + " - IntermediateTests.receivedBroadcastMessage: " + message);
            assertEquals(senderLink, message.source);
            assertArrayEquals(brdMsgToSend.get(), message.asBytes());
            nodesAndNumberOfReceivedMessages.compute(p2Link, (link, counter) -> counter == null? 1 : counter+1);
        });

        P2LNode senderNode = P2LNode.create(senderLink); //creates server thread
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
        List<P2Link> successLinks = senderNode.recursiveGarnerConnections(4, nodes[0].getSelfLink());
        assertEquals(4, successLinks.size());

        System.out.println("successLinks = " + successLinks);
        System.out.println("sending broadcast now");

        TimeDiffMarker.setMark("line + garner 4");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LBroadcastMessage.from(senderLink, P2LMessage.with(0, brdMsgToSend.get())));
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

        printPeers(senderNode);
        assertTrue(connectAsRing(nodes).get(1000));
        assertTrue(senderNode.establishConnection(nodes[0].getSelfLink()).get(1000));
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5});
        TimeDiffMarker.setMark("sender + ring");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LBroadcastMessage.from(senderLink, P2LMessage.with(0, brdMsgToSend.get())));
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
        sendResult = senderNode.sendBroadcastWithReceipts(P2LBroadcastMessage.from(senderLink, P2LMessage.with(0, brdMsgToSend.get())));
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
        senderNode.recursiveGarnerConnections(200, nodes[0].getSelfLink(), nodes[1].getSelfLink());
        System.err.println("FULL CONNECT");
        printPeers(senderNode);
        printPeers(nodes);
        assertTrue(fullConnectFuture.get(1000));

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8,9});

        TimeDiffMarker.setMark("full");
        sendResult = senderNode.sendBroadcastWithReceipts(P2LBroadcastMessage.from(senderLink, P2LMessage.with(0, brdMsgToSend.get())));
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
        assertTrue(fullConnectFuture.get(2500));
        senderNode.recursiveGarnerConnections(200, nodes[0].getSelfLink(), nodes[1].getSelfLink());
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8,9}); //note that it is the same message as before, the hash nonetheless changes...

        sendResult = senderNode.sendBroadcastWithReceipts(P2LBroadcastMessage.from(senderLink, P2LMessage.withExpiration(10, MAX_EXPIRATION_TIMEOUT, brdMsgToSend.get()))); //IF NOT SUPPLYING A MESSAGE ID, THE OLD MESSAGES WILL BE RECEIVED HERE FIRST....
        assertEquals(10, sendResult.get(1000).intValue());

        for(P2LNode node:nodes) {
            if(new Random().nextBoolean()) {
                //no longer possible to wait for a message from a specific node - because that would not really be broadcast semantics, right?
//                assertThrows(TimeoutException.class, () -> node.expectBroadcastMessage(nodes[0].getSelfLink(), 10).get(1000));
                assertArrayEquals(brdMsgToSend.get(), node.expectBroadcastMessage(/*local(senderNode), */10).get(10000).asBytes());
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




        //TODO - broadcast test that is guaranteed to use intermediate hashes
    }

    @Test void messagePassingWithTypeConvertedFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62000);
        ArrayList<P2LFuture<Integer>> fs = new ArrayList<>();
        for(int i=0;i<3;i++) {
            nodes[0].sendMessage(nodes[1].getSelfLink(), P2LMessage.with(i, new byte[1]));
            fs.add(nodes[1].expectMessage(i).toType(m -> 1));
        }
        Integer result = P2LFuture.reduce(fs, P2LFuture.PLUS).get(1000);
        assertEquals(3, result.intValue());
        close(nodes);
    }
    @Test void messagePassingWithEarlyFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62010);
        P2LFuture<ReceivedP2LMessage> earlyFuture = nodes[1].expectMessage(1);
        sleep(500);
        nodes[0].sendMessage(nodes[1].getSelfLink(), P2LMessage.with(1, BitHelper.getBytes(142)));
        assertEquals(142, earlyFuture.get(100).nextInt());
        close(nodes);
    }
    @Test void messagePassingWithExpiredFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62020);
        assertThrows(TimeoutException.class, () -> nodes[1].expectMessage(1).get(20)); //expires, and the internal message queue throws it away
        sleep(500);
        nodes[0].sendMessage(nodes[1].getSelfLink(), P2LMessage.with(1, BitHelper.getBytes(142)));
        assertEquals(142, nodes[1].expectMessage(1).get(100).nextInt());
        close(nodes);
    }
    @Test void messagePassingWithCanceledFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 62030);
        AtomicReference<P2LFuture<ReceivedP2LMessage>> future = new AtomicReference<>();
        AtomicInteger successCounter = new AtomicInteger(0);
        new Thread(() -> {
            future.set(nodes[1].expectMessage(1));
            assertThrows(CanceledException.class, ()->future.get().get(500));
            successCounter.getAndIncrement();
        }).start();
        sleep(50);
        future.get().cancel();
//        sleep(500);
        nodes[0].sendMessage(nodes[1].getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, BitHelper.getBytes(142)));
        sleep(500);//ensure other, canceled future could theoretically receive it, before the next line takes precedence
        assertEquals(142, nodes[1].expectMessage(1).get(100).nextInt());
        assertEquals(1, successCounter.get());
        close(nodes);
    }
    @Test void messagePassingWithCallMeBackFutureWorks() throws IOException {
        P2LNode[] nodes = generateNodes(2, 60041);
        nodes[1].expectMessage(1).callMeBack(m -> {
            System.out.println("CALLED!!!!!");
            assertEquals(142, m.nextInt());
        });
        sleep(500);
        nodes[0].sendMessage(nodes[1].getSelfLink(), P2LMessage.with(1, BitHelper.getBytes(142)));
        sleep(500); //wait, until the message is actually received by the callback (since the internal message queue prefers the latest registered receiver(STACK))
        assertThrows(TimeoutException.class, () -> nodes[1].expectMessage(1).get(100));
        close(nodes);
    }

    @Test void stillWorksWithDroppedPackagesTest() throws IOException {
        DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 10;

        try {
            P2LNode[] nodes = generateNodes(10, 61408);

//        P2LFuture<Boolean> f = fullConnect(nodes);
//        assertTrue(f.get());
//        printPeers(nodes);


            connectAsRing(nodes);
            nodes[0].establishConnections(nodes[2].getSelfLink(), nodes[4].getSelfLink(), nodes[5].getSelfLink(), nodes[7].getSelfLink()).waitForIt();
            nodes[2].establishConnections(nodes[9].getSelfLink(), nodes[6].getSelfLink(), nodes[8].getSelfLink(), nodes[7].getSelfLink()).waitForIt();
            nodes[5].establishConnections(nodes[1].getSelfLink(), nodes[2].getSelfLink(), nodes[9].getSelfLink(), nodes[8].getSelfLink()).waitForIt();
            nodes[6].establishConnections(nodes[0].getSelfLink(), nodes[2].getSelfLink(), nodes[9].getSelfLink(), nodes[7].getSelfLink()).waitForIt();
            nodes[8].establishConnections(nodes[3].getSelfLink()).waitForIt();
            printPeers(nodes);

            for (P2LNode node : nodes)
                node.addBroadcastListener(message -> System.out.println("message = " + message));

            nodes[0].sendMessageWithRetries(nodes[1].getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, "sup".getBytes(UTF_8)), 3, 500); //not even an established connection
            nodes[0].sendMessageWithRetries(nodes[2].getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, "sup".getBytes(UTF_8)), 3, 500);
            assertEquals("sup", nodes[1].expectMessage(1).get(1).asString());
            assertEquals("sup", nodes[2].expectMessage(1).get(1).asString());
            assertThrows(TimeoutException.class, () -> nodes[3].expectMessage(1).get(1));

            nodes[6].sendBroadcastWithReceipts(P2LBroadcastMessage.from(nodes[6].getSelfLink(), P2LMessage.withExpiration(1, MAX_EXPIRATION_TIMEOUT, "sup".getBytes(UTF_8)))).waitForIt(20000);

            for (P2LNode node : nodes)
                if (node != nodes[6])
                    node.expectBroadcastMessage(1).waitForIt(2500);

            close(nodes);
        } finally {
            DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
        }
    }

    @Test public void broadcastPackingTest() {
        P2LBroadcastMessage orig = P2LBroadcastMessage.from(new P2Link.Local("test", 10004), P2LMessage.withExpiration(12, MAX_EXPIRATION_TIMEOUT, new byte[] {1,2,3,4,5}));

        P2LMessageHeader containerHeader = new MinimalHeader((short) 2, false);
        MessageEncoder encoder = containerHeader.encoder();
        orig.packInto(encoder);
        P2LMessage container = P2LMessage.from(containerHeader, encoder);

        P2LBroadcastMessage decoded = P2LBroadcastMessage.unpackFrom(container);

        assertEquals(orig, decoded);
    }

    @Test void broadcastWithAndWithoutHash() throws IOException {
        P2LHeuristics.BROADCAST_USES_HASH_DETOUR_RAW_SIZE_THRESHOLD = 100;
        try {
            int sameType = 1234;

            P2LNode[] nodes = generateNodes(6, 37412);
            fullConnect(nodes).waitForIt(2500);

            TimeDiffMarker.setMark_d();
            byte[] shortPayload = {1, 9, 2, 8, 3, 7, 4, 6, 5};
            nodes[0].sendBroadcastWithReceipts(P2LBroadcastMessage.from(nodes[0].getSelfLink(), P2LMessage.withExpiration(sameType, MAX_EXPIRATION_TIMEOUT, shortPayload)));
            for (P2LNode node : nodes)
                if (node != nodes[0])
                    assertArrayEquals(shortPayload, node.expectBroadcastMessage(sameType).get(2500).asBytes());

            TimeDiffMarker.println_setMark_d();

            byte[] longPayload = new byte[P2LHeuristics.BROADCAST_USES_HASH_DETOUR_RAW_SIZE_THRESHOLD * 2];
            ThreadLocalRandom.current().nextBytes(longPayload);
            nodes[0].sendBroadcastWithReceipts(P2LBroadcastMessage.from(nodes[0].getSelfLink(), P2LMessage.withExpiration(sameType, MAX_EXPIRATION_TIMEOUT, longPayload)));
            for (P2LNode node : nodes)
                if (node != nodes[0])
                    assertArrayEquals(longPayload, node.expectBroadcastMessage(sameType).get(2500).asBytes());

            TimeDiffMarker.println_d();
        } finally {
            P2LHeuristics.BROADCAST_USES_HASH_DETOUR_RAW_SIZE_THRESHOLD = P2LMessage.CUSTOM_RAW_SIZE_LIMIT;
        }
    }

    @Test @Disabled void stressTest() {

        //todo do CRAZY STUFF

        //do a simple broadcast test to check whether that still works after all the commotion...
    }





    @Test @Disabled void streamWithDroppedPackagesTest() {
        //todo - though currently congestion control is so terrible that many packages are dropped either way
    }


    @Test void testDatagramBehaviour() throws IOException {
        DatagramSocket send_socket = new DatagramSocket(1025);
        DatagramSocket recv_socket = new DatagramSocket(1026);

        byte[] send_buf = new byte[10];
        ThreadLocalRandom.current().nextBytes(send_buf);
        DatagramPacket send_packet = new DatagramPacket(send_buf, send_buf.length, new InetSocketAddress("localhost", 1026));
        send_socket.send(send_packet);

        byte[] recv_buf = new byte[5];
        DatagramPacket recv_packet = new DatagramPacket(recv_buf, recv_buf.length);
        recv_socket.receive(recv_packet);

        System.out.println("recv_packet.getData() = " + Arrays.toString(recv_packet.getData()));
        System.out.println("recv_packet.getOffset() = " + recv_packet.getOffset());
        System.out.println("recv_packet.getLength() = " + recv_packet.getLength());
    }



    @Test void testTestableConnectionCombinations() throws IOException {
        P2LNode pubNode1 = null;
        P2LNode pubNode2 = null;
        P2LNode hidNode1 = null;
        P2LNode hidNode2 = null;
        P2LNode pubRelayNode3 = null;
        try {
//            public - public
            pubNode1 = NodeCreator.create(Local.forTest(7890));
            pubNode2 = NodeCreator.create(Local.forTest(7891));

            P2LFuture<Boolean> pubPubConFut = pubNode1.establishConnection(pubNode2.getSelfLink());
            assertTrue(pubPubConFut.get(1000));

            close(pubNode1, pubNode2);

            //hidden - public
            hidNode1 = NodeCreator.create(Local.forTest(7890));
            pubNode2 = NodeCreator.create(Local.forTest(7891));

            P2LFuture<Boolean> hidPubConFut = hidNode1.establishConnection(pubNode2.getSelfLink());
            assertTrue(hidPubConFut.get(1000));

            close(hidNode1, pubNode2);

            //public - hidden (using relay)
            pubNode1 = NodeCreator.create(Local.forTest(7890));
            hidNode2 = NodeCreator.create(Local.forTest(7891));

            //not possible: hidden node is hidden, link is unknown
//        P2LFuture<Boolean> pubHidConFut = pubNode1.establishConnection(hidNode2.getSelfLink());
//        assertTrue(pubHidConFut.get(1000));
            pubRelayNode3 = NodeCreator.create(Local.forTest(7892));
            P2LFuture<Boolean> pubRelayConFut = pubNode1.establishConnection(pubRelayNode3.getSelfLink());
            P2LFuture<Boolean> hidRelayConFut = hidNode2.establishConnection(pubRelayNode3.getSelfLink());
            assertTrue(pubRelayConFut.get(1000) && hidRelayConFut.get(1000));

            List<P2Link> linksQueriedFromRelayNode = pubNode1.queryKnownLinksOf(pubRelayNode3.getSelfLink());
            System.out.println("linksQueriedFromRelayNode(pubHid) = " + linksQueriedFromRelayNode);
            assertEquals(1, linksQueriedFromRelayNode.size()); //NOTE: only equal to 1, because the requesting link is obviously filtered from queried list
            P2LFuture<Boolean> pubHidConFut = pubNode1.establishConnection(linksQueriedFromRelayNode.get(0));//automatically establishes a connection - through a reverse connection from hid to pub
            assertTrue(pubHidConFut.get(2500));

            close(pubNode1, hidNode2, pubRelayNode3);

            //hidden - hidden (using relay)
            hidNode1 = NodeCreator.create(Local.forTest(7890));
            hidNode2 = NodeCreator.create(Local.forTest(7891));
            pubRelayNode3 = NodeCreator.create(Local.forTest(7892));
            P2LFuture<Boolean> hidRelayConFut1 = hidNode1.establishConnection(pubRelayNode3.getSelfLink());
            P2LFuture<Boolean> hidRelayConFut2 = hidNode2.establishConnection(pubRelayNode3.getSelfLink());
            assertTrue(hidRelayConFut1.get(1000) && hidRelayConFut2.get(1000));

            linksQueriedFromRelayNode = hidNode1.queryKnownLinksOf(pubRelayNode3.getSelfLink());
            System.out.println("linksQueriedFromRelayNode(hidHid) = " + linksQueriedFromRelayNode);
            assertEquals(1, linksQueriedFromRelayNode.size());
            P2LFuture<Boolean> hidHidConFut = hidNode1.establishConnection(linksQueriedFromRelayNode.get(0));//automatically establishes a connection - through a reverse connection from hid to pub
            assertTrue(hidHidConFut.get(2500));

            close(hidNode1, hidNode2, pubRelayNode3);
        } finally {
            close(pubNode1, pubNode2, hidNode1, hidNode2, pubRelayNode3);
        }
    }

    @Test void conversationIdTest() throws IOException {
        P2LNodeInternal parent = (P2LNodeInternal) NodeCreator.create(Local.forTest(60943));
        for(int i=0;i<Short.MAX_VALUE*2;i++) {
            short gen = RelayedConnectionProtocol.createConversationForInitialDirect(parent);
            assertTrue(gen < 0);
        }

        for(int i=0;i<Short.MAX_VALUE*2;i++) {
            short gen = RelayedConnectionProtocol.createConversationForRelay(parent);
            assertTrue(gen > 0);
        }
    }

    public static void streamSplitAssertions(InputStream stream, String toSend, boolean forceClose) {
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
    static List<P2LMessage> toBytesAndSplitRandomly(String toSend, int packetCount) {
        ArrayList<P2LMessage> packets = new ArrayList<>(packetCount);
        byte[] send = new LITypeToBytesTransformer().transform(toSend);

        int numBytesSend = 0;
        for(int i=0;i<packetCount;i++) {
            P2LMessageHeader h;
            int numBytesThisPacket;
            int remaining = send.length-numBytesSend;
            if(i+1==packetCount) {
                h = new StreamPartHeader((short) 1, NO_CONVERSATION_ID, NO_STEP, i, false, true);
                numBytesThisPacket = remaining;
            } else {
                h = new StreamPartHeader((short) 1, NO_CONVERSATION_ID, NO_STEP, i, false, false);
                numBytesThisPacket = Math.min(remaining, rand(0, remaining/Math.max(1, (packetCount/4)) +2));
            }
            byte[] thisPacket = Arrays.copyOfRange(send, numBytesSend, numBytesSend+numBytesThisPacket);
            numBytesSend+=numBytesThisPacket;

            packets.add(h.generateMessage(thisPacket));
        }
        return packets;
    }


    static void printPeers(P2LNode... nodes) {
        for (int i = 0; i < nodes.length; i++)
            System.out.println("node " + i + "("+nodes[i].getSelfLink()+") peers("+nodes[i].getEstablishedConnections().length+"): " + Arrays.toString(nodes[i].getEstablishedConnections()));
    }

    public static P2LNode[] generateNodes(int size, int startPort) throws IOException {
        return generateNodes(size, startPort, null, null);
    }
    static P2LNode[] generateNodes(int size, int startPort, Function<P2Link, P2LNode.P2LMessageListener<ReceivedP2LMessage>> idvListenerCreator, Function<P2Link, P2LNode.P2LMessageListener<P2LBroadcastMessage>> brdListenerCreator) throws IOException {
        P2LNode[] nodes = new P2LNode[size];
        P2Link[] links = new P2Link[nodes.length];
        for(int i=0;i<nodes.length;i++) {
            int port = startPort + i;
            links[i] = Local.forTest(port).unsafeAsDirect();
            nodes[i] = P2LNode.create(links[i]);
            if(idvListenerCreator != null)
                nodes[i].addMessageListener(idvListenerCreator.apply(links[i]));
            if(brdListenerCreator != null)
                nodes[i].addBroadcastListener(brdListenerCreator.apply(links[i]));
        }
        return nodes;
    }
    static P2LFuture<Boolean> connectAsLine(P2LNode... nodes) {
        ArrayList<P2LFuture<Boolean>> connectionFutures = new ArrayList<>(nodes.length);
        for(int i=0; i<nodes.length-1; i++) {
            connectionFutures.add(nodes[i].establishConnection(nodes[i + 1].getSelfLink()));
            sleep(10);
        }
        return P2LFuture.reduce(connectionFutures, P2LFuture.AND);
    }
    static P2LFuture<Boolean> connectAsRing(P2LNode... nodes) {
        P2LFuture<Boolean> connectedAsLine = connectAsLine(nodes);
        return nodes[0].establishConnection(nodes[nodes.length-1].getSelfLink()).combine(connectedAsLine, P2LFuture.AND);
    }
    static P2LFuture<Boolean> connectAsRing(P2LNode node0, P2LNode... nodes) {
        P2LNode[] allNodes = new P2LNode[nodes.length+1];
        allNodes[0] = node0;
        System.arraycopy(nodes, 0, allNodes, 1, nodes.length);
        return connectAsRing(allNodes);
    }
    static P2LFuture<Boolean> fullConnect(P2LNode... nodes) {
        ArrayList<P2LFuture<Boolean>> connectionFutures = new ArrayList<>(nodes.length);
        for(int i=0; i<nodes.length; i++)
            for (int ii = i + 1; ii < nodes.length; ii++) {
                connectionFutures.add(nodes[ii].establishConnection(nodes[i].getSelfLink()));
                sleep(10);
            }
        return P2LFuture.reduce(connectionFutures, P2LFuture.AND);
    }
    static void close(P2LNode... nodes) {
        for(P2LNode node:nodes)
            if(node!=null)
                node.close();
    }
}