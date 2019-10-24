import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import org.junit.jupiter.api.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
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
        int x = p2lF.get();
        System.out.println("t1("+System.currentTimeMillis()+") - x = "+x);
    }
    @Test public void p2lFutureTest_MULTIPLE_GET() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        p2lF.callMeBack(x -> System.out.println("t1 callback: x="+x));
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get();
            System.out.println("t2("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t3("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get();
            System.out.println("t3("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t4("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get();
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
        int x = p2lF.get();
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

        assertThrows(TimeoutException.class, () -> {
            p2lF.get(500);
        });
    }



    @Test public void establishConnectionProtocolTest() {
        P2Link link1 = new P2Link("localhost", 53189);
        P2Link link2 = new P2Link("localhost", 53188);

        P2LNode node1 = P2LNode.create(link1); //creates server thread
        P2LNode node2 = P2LNode.create(link2); //creates server thread

        boolean connected = node2.connectToPeer(node1.getSelfLink());
        assertTrue(connected);

        assertTrue(node1.isConnectedTo(node2.getSelfLink()));
        assertTrue(node2.isConnectedTo(node1.getSelfLink()));
    }



    @Test public void garnerConnectionProtocolTest() {
        {
            P2LNode[] nodes = generateNodes(10, P2Link.DEFAULT_PORT);

            P2Link[] subLinks = new P2Link[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = nodes[i + 2].getSelfLink();
            Set<P2Link> successes = nodes[1].connectToPeers(subLinks);
            for (P2Link toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            List<P2Link> newConnections = nodes[0].recursiveGarnerConnections(4, nodes[1].getSelfLink());

            printPeers(nodes);

            assertEquals(4, newConnections.size());

            assertEquals(4, nodes[0].getActivePeerLinks().size());

        }


        {
            P2LNode[] nodes = generateNodes(5, 17681);

            P2Link[] subLinks = new P2Link[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = nodes[i + 2].getSelfLink();
            Set<P2Link> successes = nodes[1].connectToPeers(subLinks);
            for (P2Link toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            List<P2Link> newConnections = nodes[0].recursiveGarnerConnections(4, nodes[1].getSelfLink());
            assertEquals(4, newConnections.size());

            printPeers(nodes);

            assertEquals(4, nodes[0].getActivePeerLinks().size());
            assertEquals(4, nodes[1].getActivePeerLinks().size());
            assertEquals(2, nodes[2].getActivePeerLinks().size());
            assertEquals(2, nodes[3].getActivePeerLinks().size());
            assertEquals(2, nodes[4].getActivePeerLinks().size());
        }

        {
            P2LNode[] nodes = generateNodes(5, 17651);

            P2Link[] subLinks = new P2Link[nodes.length - 2];
            for (int i = 0; i < subLinks.length; i++) subLinks[i] = nodes[i + 2].getSelfLink();
            Set<P2Link> successes = nodes[1].connectToPeers(subLinks);
            for (P2Link toBeConnected : subLinks)
                assertTrue(successes.contains(toBeConnected));

            List<P2Link> newConnections = nodes[0].recursiveGarnerConnections(1000, nodes[1].getSelfLink());
            assertEquals(4, newConnections.size());

            printPeers(nodes);

            assertEquals(4, nodes[0].getActivePeerLinks().size());
            assertEquals(4, nodes[1].getActivePeerLinks().size());
            assertEquals(2, nodes[2].getActivePeerLinks().size());
            assertEquals(2, nodes[3].getActivePeerLinks().size());
            assertEquals(2, nodes[4].getActivePeerLinks().size());
        }
    }

    private void printPeers(P2LNode... nodes) {
        for (int i = 0; i < nodes.length; i++)
            System.out.println("node " + i + "("+nodes[i].getSelfLink()+") peers: " + nodes[i].getActivePeerLinks());
    }

    private P2LNode[] generateNodes(int size, int startPort) {
        return generateNodes(size, startPort, null, null);
    }
    private P2LNode[] generateNodes(int size, int startPort, Function<P2Link, P2LNode.P2LMessageListener> idvListenerCreator, Function<P2Link, P2LNode.P2LMessageListener> brdListenerCreator) {
        P2LNode[] nodes = new P2LNode[size];
        P2Link[] links = new P2Link[nodes.length];
        for(int i=0;i<nodes.length;i++) {
            links[i] = new P2Link("localhost", startPort + i);
            nodes[i] = P2LNode.create(links[i]);
            if(idvListenerCreator != null)
                nodes[i].addIndividualMessageListener(idvListenerCreator.apply(links[i]));
            if(brdListenerCreator != null)
                nodes[i].addBroadcastListener(brdListenerCreator.apply(links[i]));
        }
        return nodes;
    }


    @Test public void individualMessageTest() {
        Map<P2Link, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        byte[] idvMsgToSend = new byte[] {17,32,37,45,5,99,33,55,16,43,127};

        P2Link l1 = new P2Link("localhost", 54189);
        P2Link l2 = new P2Link("localhost", 54188);
        P2LNode node1 = P2LNode.create(l1); //creates server thread
        node1.addIndividualMessageListener(message -> {
            assertEquals(l2, message.sender);
            assertArrayEquals(idvMsgToSend, message.data);
            nodesAndNumberOfReceivedMessages.compute(l1, (link, counter) -> counter == null? 1 : counter+1);
        });
        node1.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });
        P2LNode node2 = P2LNode.create(l2); //creates server thread
        node2.addIndividualMessageListener(message -> {
            assertEquals(l1, message.sender);
            assertArrayEquals(idvMsgToSend, message.data);
            nodesAndNumberOfReceivedMessages.compute(l2, (link, counter) -> counter == null? 1 : counter+1);
        });
        node2.addBroadcastListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });

        sleep(100); //let nodes start

        boolean connected = node1.connectToPeer(node2.getSelfLink());
        assertTrue(connected);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendIndividualMessageTo(node1.getSelfLink(), idvMsgToSend);
        assertTrue(sendResult.get());
        sendResult = node1.sendIndividualMessageTo(node2.getSelfLink(), idvMsgToSend);
        assertTrue(sendResult.get());
        sendResult = node1.sendIndividualMessageTo(node1.getSelfLink(), idvMsgToSend);
        assertFalse(sendResult.get()); //self send does not work
        sendResult = node1.sendIndividualMessageTo(new P2Link("google.com", 123), idvMsgToSend);
        assertFalse(sendResult.get()); //google is not a connected peer for node 1

        System.out.println("send success");

        sleep(250);

        assertEquals(2, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);
    }


    @Test public void futureIdvMsgText() {
        P2Link l1 = new P2Link("localhost", 34189);
        P2Link l2 = new P2Link("localhost", 34188);
        P2LNode node1 = P2LNode.create(l1); //creates server thread
        P2LNode node2 = P2LNode.create(l2); //creates server thread

        sleep(100); //let nodes start

        boolean connected = node1.connectToPeer(node2.getSelfLink());
        assertTrue(connected);

        printPeers(node1, node2);

        P2LFuture<Boolean> sendResult;
        sendResult = node2.sendIndividualMessageTo(node1.getSelfLink(), 1, P2LMessage.fromString("hallo"));
        assertTrue(sendResult.get());
        sendResult = node1.sendIndividualMessageTo(node2.getSelfLink(), 1, P2LMessage.fromString("welt"));
        assertTrue(sendResult.get());

        String node2Received = node2.expectIndividualMessage(1).get().asString();
        assertEquals("welt", node2Received);
        String node1Received = node1.expectIndividualMessage(1).get().asString();
        assertEquals("hallo", node1Received);

        sendResult = node2.sendIndividualMessageTo(node1.getSelfLink(), 25, P2LMessage.fromString("hallo welt!"));
        assertTrue(sendResult.get());

        assertThrows(TimeoutException.class, () -> {
            node1.expectIndividualMessage(1).get(100); //will timeout, because message was consumed
        });

        sendResult = node2.sendIndividualMessageTo(node1.getSelfLink(), 1, P2LMessage.fromString("hallo welt"));
        assertTrue(sendResult.get());
        String node1Received2 = node1.expectIndividualMessage(1).get(100).asString(); //no longer times out, because node 2 has send another message now
        assertEquals("hallo welt", node1Received2);

        assertThrows(TimeoutException.class, () -> {
            node1.expectIndividualMessage(node1.getSelfLink(), 25).get(100).asString(); //will timeout, because the message is not from node1...
        });
        String node1Received3 = node1.expectIndividualMessage(node2.getSelfLink(), 25).get(100).asString();
        assertEquals("hallo welt!", node1Received3);
    }


    @Test public void broadcastMessageTest() {
        Map<P2Link, Integer> nodesAndNumberOfReceivedMessages = new ConcurrentHashMap<>();

        AtomicReference<byte[]> brdMsgToSend = new AtomicReference<>(new byte[] {17,32,37,45,5,99,33,55,16,43,127});

        P2Link senderLink = new P2Link("localhost", 55199);

        P2LNode[] nodes = generateNodes(10, 55288, p2Link -> message -> {
            throw new IllegalStateException("this should not be called here");
        }, p2Link -> message -> {
            System.out.println(p2Link + " - IntermediateTests.receivedBroadcastMessage: " + message);
            assertEquals(senderLink, message.sender);
            assertArrayEquals(brdMsgToSend.get(), message.data);
            nodesAndNumberOfReceivedMessages.compute(p2Link, (link, counter) -> counter == null? 1 : counter+1);
        });

        P2LNode senderNode = P2LNode.create(senderLink); //creates server thread
        senderNode.addBroadcastListener(message -> {
            throw new IllegalStateException("broadcastReceivedCalledForSender...");
        });
        senderNode.addIndividualMessageListener(message -> {
            throw new IllegalStateException("this should not be called here");
        });

        sleep(100); //let nodes start

        assertTrue(connectAsLine(nodes));
        List<P2Link> successLinks = senderNode.recursiveGarnerConnections(4, nodes[0].getSelfLink());
        assertEquals(4, successLinks.size());

        System.out.println("successLinks = " + successLinks);
        printPeers(senderNode);
        printPeers(nodes);
        System.out.println("sending broadcast now");

        TimeDiffMarker.setMark("line + garner 4");
        P2LFuture<Integer> sendResult;
        sendResult = senderNode.sendBroadcast(brdMsgToSend.get());
        assertEquals(new Integer(4), sendResult.get());

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


        senderNode.disconnect();
        for(P2LNode node : nodes) node.disconnect();
        nodesAndNumberOfReceivedMessages.clear();

        assertTrue(connectAsRing(nodes));
        assertTrue(senderNode.connectToPeer(nodes[0].getSelfLink()));
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5});
        TimeDiffMarker.setMark("sender + ring");
        sendResult = senderNode.sendBroadcast(brdMsgToSend.get());
        assertEquals(new Integer(1), sendResult.get());

        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
        }
        TimeDiffMarker.println("sender + ring");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);


        senderNode.disconnect();
        for(P2LNode node : nodes) node.disconnect();
        nodesAndNumberOfReceivedMessages.clear();

        assertTrue(connectAsRing(senderNode, nodes));
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8});
        TimeDiffMarker.setMark("ring");
        sendResult = senderNode.sendBroadcast(brdMsgToSend.get());
        assertEquals(new Integer(2), sendResult.get());


        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }
        TimeDiffMarker.println("ring");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);



        senderNode.disconnect();
        for(P2LNode node : nodes) node.disconnect();
        nodesAndNumberOfReceivedMessages.clear();

        assertTrue(fullConnect( nodes));
        senderNode.recursiveGarnerConnections(200, nodes[0].getSelfLink(), nodes[1].getSelfLink());
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8,9});

        TimeDiffMarker.setMark("full");
        sendResult = senderNode.sendBroadcast(brdMsgToSend.get());
        assertEquals(new Integer(10), sendResult.get());


        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }
        TimeDiffMarker.println("full");

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);



        senderNode.disconnect();
        for(P2LNode node : nodes) node.disconnect();
        nodesAndNumberOfReceivedMessages.clear();

        assertTrue(fullConnect( nodes));
        senderNode.recursiveGarnerConnections(200, nodes[0].getSelfLink(), nodes[1].getSelfLink());
        printPeers(senderNode);
        printPeers(nodes);

        brdMsgToSend.set(new byte[] {1,2,3,4,5,6,7,8,9}); //note that it is the same message as before, the hash nonetheless changes...

        sendResult = senderNode.sendBroadcast(10, brdMsgToSend.get()); //IF NOT SUPPLYING A MESSAGE ID, THE OLD MESSAGES WILL BE RECEIVED HERE FIRST....
        assertEquals(new Integer(10), sendResult.get());

        for(P2LNode node:nodes) {
            if(new Random().nextBoolean()) {
                assertThrows(TimeoutException.class, () -> {
                    node.expectBroadcastMessage(nodes[0].getSelfLink(), 10).get(1000);
                });
                assertArrayEquals(brdMsgToSend.get(), node.expectBroadcastMessage(senderNode.getSelfLink(), 10).get(10000).data);
            } else {
                assertArrayEquals(brdMsgToSend.get(), node.expectBroadcastMessage(10).get(10000).data);
            }
        }
        assertThrows(TimeoutException.class, () -> {
            senderNode.expectBroadcastMessage(10).get(1000); //sender node will be the only one that does not receive the broadcast (from itself)
            nodes[0].expectBroadcastMessage(10).get(1000); //also times out, because the message has been consumed..
        });

        while(nodesAndNumberOfReceivedMessages.size() < nodes.length) {
            sleep(10);
//            System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        }

        System.out.println("nodesAndNumberOfReceivedMessages = " + nodesAndNumberOfReceivedMessages);
        assertEquals(nodes.length, nodesAndNumberOfReceivedMessages.size());
        for(Integer numberOfReceivedMessages : nodesAndNumberOfReceivedMessages.values())
            assertEquals(new Integer(1), numberOfReceivedMessages);
    }


    @Test public void stressTest() {

        //todo do CRAZY STUFF

        //do a simple broadcast test to check whether that still works after all the commotion...

        throw new NotImplementedException();
    }


    private boolean connectAsLine(P2LNode... nodes) {
        for(int i=0; i<nodes.length-1; i++) {
            boolean connected = nodes[i].connectToPeer(nodes[i+1].getSelfLink());
            if(!connected)
                return false;
        }
        return true;
    }
    private boolean connectAsRing(P2LNode... nodes) {
        boolean connectedAsLine = connectAsLine(nodes);
        if(connectedAsLine)
            return nodes[0].connectToPeer(nodes[nodes.length-1].getSelfLink());
        return false;
    }
    private boolean connectAsRing(P2LNode node0, P2LNode[] nodes) {
        P2LNode[] allNodes = new P2LNode[nodes.length+1];
        allNodes[0] = node0;
        System.arraycopy(nodes, 0, allNodes, 1, nodes.length);
        return connectAsRing(allNodes);
    }
    private boolean fullConnect(P2LNode[] nodes) {
        for(int i=0; i<nodes.length-1; i++) {
            for(int ii=0;ii<nodes.length;ii++) {
                if(i!=ii) {
                    boolean connected = nodes[i].connectToPeer(nodes[ii].getSelfLink());
                    if (!connected)
                        return false;
                }
            }
        }
        return true;
    }
}