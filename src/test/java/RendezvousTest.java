import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.P2Link.Local;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.core.NodeCreator;
import jokrey.utilities.network.link2peer.rendezvous.IdentityTriple;
import jokrey.utilities.network.link2peer.rendezvous.RendezvousServer;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static jokrey.utilities.simple.data_structure.stack.ConcurrentStackTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jokrey
 */
class RendezvousTest {
    @Test
    public void identityTripleEncodeTest() {
        IdentityTriple orig = new IdentityTriple("1", new byte[0], new P2Link.Direct("localhost", 10002));

        MessageEncoder encoder = new MessageEncoder(200);
        orig.encodeInto(encoder, true);
        encoder.resetPointer();

        IdentityTriple decoded = IdentityTriple.decodeNextOrNull(encoder);

        assertEquals(orig, decoded);

        IdentityTriple d2 = IdentityTriple.decodeNextOrNull(encoder);
        assertNull(d2);
    }

    @Test
    public void rendezvousTest1() throws IOException {
        P2Link.Direct rendezvousServerLink = Local.forTest(40000).unsafeAsDirect();
        try(RendezvousServer server = new RendezvousServer(rendezvousServerLink)) {
            P2LNode node1 = NodeCreator.create(Local.forTest(30001));
            P2LNode node2 = NodeCreator.create(Local.forTest(30002));

            //note - two connections can be established to the other at the exact same millisecond. If Conversations cannot handle that it fails - but they can since 2.2
            P2LFuture<IdentityTriple> fut1 = P2LThreadPool.executeSingle(() -> RendezvousServer.rendezvousWith(node1, rendezvousServerLink, new IdentityTriple("1", new byte[] {}, node1.getSelfLink()), 5000, "2")[0]);
            P2LFuture<IdentityTriple> fut2 = P2LThreadPool.executeSingle(() -> RendezvousServer.rendezvousWith(node2, rendezvousServerLink, new IdentityTriple("2", new byte[] {}, node2.getSelfLink()), 5000, "1")[0]);

            IdentityTriple idOf1KnownTo2 = fut2.get(2500);
            IdentityTriple idOf2KnownTo1 = fut1.get(2500);
            assertNotNull(idOf1KnownTo2);
            assertNotNull(idOf2KnownTo1);

            assertTrue(node1.isConnectedTo(node2.getSelfLink()));
            assertTrue(node2.isConnectedTo(node1.getSelfLink()));


            shortBackAndForthTest(node1, node2, idOf1KnownTo2, idOf2KnownTo1);

            node1.close();
            node2.close();
        }
    }

    @Test
    public void rendezvousTest2() throws IOException {
        P2Link.Direct rendezvousServerLink = Local.forTest(40000).unsafeAsDirect();
        try(RendezvousServer server = new RendezvousServer(rendezvousServerLink)) {
            P2LNode node1 = NodeCreator.create(Local.forTest(30003));
            P2LNode node2 = NodeCreator.create(Local.forTest(30004));

            P2LFuture<IdentityTriple> fut1 = P2LThreadPool.executeSingle(() -> {
                sleep(1000);
                return RendezvousServer.rendezvousWith(node1, rendezvousServerLink, new IdentityTriple("1", new byte[] {}, node1.getSelfLink()), 5000, "2")[0];
            });
            P2LFuture<IdentityTriple> fut2 = P2LThreadPool.executeSingle(() -> {
                sleep(250);
                return RendezvousServer.rendezvousWith(node2, rendezvousServerLink, new IdentityTriple("2", new byte[] {}, node2.getSelfLink()), 5000, "1")[0];
            });

            IdentityTriple idOf1KnownTo2 = fut2.getOrNull(8000);
            IdentityTriple idOf2KnownTo1 = fut1.getOrNull(8000);
            System.out.println("idOf1KnownTo2 = " + idOf1KnownTo2);
            System.out.println("idOf2KnownTo1 = " + idOf2KnownTo1);
            assertNotNull(idOf1KnownTo2);
            assertNotNull(idOf2KnownTo1);

            assertTrue(node1.isConnectedTo(node2.getSelfLink()));
            assertTrue(node2.isConnectedTo(node1.getSelfLink()));

            shortBackAndForthTest(node1, node2, idOf1KnownTo2, idOf2KnownTo1);

            node1.close();
            node2.close();
        }
    }

    @Test
    public void rendezvousTest3() throws IOException {
        P2Link.Direct rendezvousServerLink = Local.forTest(40000).unsafeAsDirect();
        try(RendezvousServer server = new RendezvousServer(rendezvousServerLink)) {
            P2LNode node1 = NodeCreator.create(Local.forTest(30005));
            P2LNode node2 = NodeCreator.create(Local.forTest(30006));

            P2LFuture<IdentityTriple> fut1 = P2LThreadPool.executeSingle(() -> {
                sleep(2500);
                return RendezvousServer.rendezvousWith(node1, rendezvousServerLink, new IdentityTriple("1", new byte[] {}, node1.getSelfLink()), 5000, "2")[0];
            });
            P2LFuture<IdentityTriple> fut2 = P2LThreadPool.executeSingle(() -> {
                while(true) {
                    try {
                        sleep(250);
                        return RendezvousServer.rendezvousWith(node2, rendezvousServerLink, new IdentityTriple("2", new byte[]{}, node2.getSelfLink()), 5000, "1")[0];
                    } catch (TimeoutException e) {} //one timeout exception is expected
                }
            });

            IdentityTriple idOf1KnownTo2 = fut2.getOrNull(5000);
            IdentityTriple idOf2KnownTo1 = fut1.getOrNull(5000);
            System.out.println("idOf1KnownTo2 = " + idOf1KnownTo2);
            System.out.println("idOf2KnownTo1 = " + idOf2KnownTo1);
            assertNotNull(idOf1KnownTo2);
            assertNotNull(idOf2KnownTo1);

            assertTrue(node1.isConnectedTo(node2.getSelfLink()));
            assertTrue(node2.isConnectedTo(node1.getSelfLink()));

            shortBackAndForthTest(node1, node2, idOf1KnownTo2, idOf2KnownTo1);

            node1.close();
            node2.close();
        }
    }

    private void shortBackAndForthTest(P2LNode node1, P2LNode node2, IdentityTriple idOf1KnownTo2, IdentityTriple idOf2KnownTo1) throws IOException {
        P2LFuture<ReceivedP2LMessage> msgFut = node1.expectMessage(123);

        P2LMessage msgToSend = P2LMessage.with(123);
        msgToSend.encode(-432);
        node2.sendMessage(idOf1KnownTo2.link, msgToSend);

        assertEquals(-432, msgFut.get(1000).nextInt());
    }
}