import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.P2Link.Local;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.core.NodeCreator;
import jokrey.utilities.network.link2peer.rendevouz.RendezvousServer;
import jokrey.utilities.network.link2peer.rendevouz.RendezvousServer.IdentityTriple;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import org.junit.jupiter.api.Test;
import java.io.IOException;

import static jokrey.utilities.simple.data_structure.stack.ConcurrentStackTest.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 *
 * @author jokrey
 */
class RendezvousTest {
    @Test
    public void rendezvousTest1() throws IOException {
        P2Link rendezvousServerLink = Local.forTest(40000);
        try(RendezvousServer server = new RendezvousServer(rendezvousServerLink)) {
            P2LNode node1 = NodeCreator.create(Local.forTest(30001));
            P2LNode node2 = NodeCreator.create(Local.forTest(30002));

            P2LFuture<IdentityTriple> fut1 = P2LThreadPool.executeSingle(() -> RendezvousServer.rendezvousWith(node1, rendezvousServerLink, new IdentityTriple("1", new byte[] {}, node1.getSelfLink()), "2")[0]);
            P2LFuture<IdentityTriple> fut2 = P2LThreadPool.executeSingle(() -> RendezvousServer.rendezvousWith(node2, rendezvousServerLink, new IdentityTriple("2", new byte[] {}, node2.getSelfLink()), "1")[0]);

            IdentityTriple idOf1KnownTo2 = fut2.get(1000);
            IdentityTriple idOf2KnownTo1 = fut1.get(1000);

            shortBackAndForthTest(node1, node2, idOf1KnownTo2, idOf2KnownTo1);

            node1.close();
            node2.close();
        }
    }

    @Test
    public void rendezvousTest2() throws IOException {
        P2Link rendezvousServerLink = Local.forTest(40000);
        try(RendezvousServer server = new RendezvousServer(rendezvousServerLink)) {
            P2LNode node1 = NodeCreator.create(Local.forTest(30003));
            P2LNode node2 = NodeCreator.create(Local.forTest(30004));

            P2LFuture<IdentityTriple> fut1 = P2LThreadPool.executeSingle(() -> {
                sleep(1000);
                return RendezvousServer.rendezvousWith(node1, rendezvousServerLink, new IdentityTriple("1", new byte[] {}, node1.getSelfLink()), "2")[0];
            });
            P2LFuture<IdentityTriple> fut2 = P2LThreadPool.executeSingle(() -> {
                sleep(250);
                return RendezvousServer.rendezvousWith(node2, rendezvousServerLink, new IdentityTriple("2", new byte[] {}, node2.getSelfLink()), "1")[0];
            });

            IdentityTriple idOf1KnownTo2 = fut2.getOrNull(8000);
            IdentityTriple idOf2KnownTo1 = fut1.getOrNull(8000);
            System.out.println("idOf1KnownTo2 = " + idOf1KnownTo2);
            System.out.println("idOf2KnownTo1 = " + idOf2KnownTo1);
            assertNotNull(idOf1KnownTo2);
            assertNotNull(idOf2KnownTo1);

            shortBackAndForthTest(node1, node2, idOf1KnownTo2, idOf2KnownTo1);

            node1.close();
            node2.close();
        }
    }

    @Test
    public void rendezvousTest3() throws IOException {
        P2Link rendezvousServerLink = Local.forTest(40000);
        try(RendezvousServer server = new RendezvousServer(rendezvousServerLink)) {
            P2LNode node1 = NodeCreator.create(Local.forTest(30005));
            P2LNode node2 = NodeCreator.create(Local.forTest(30006));

            P2LFuture<IdentityTriple> fut1 = P2LThreadPool.executeSingle(() -> {
                sleep((long) (RendezvousServer.CALLBACK_TIMEOUT * 1.5));
                return RendezvousServer.rendezvousWith(node1, rendezvousServerLink, new IdentityTriple("1", new byte[] {}, node1.getSelfLink()), "2")[0];
            });
            P2LFuture<IdentityTriple> fut2 = P2LThreadPool.executeSingle(() -> {
                while(true) {
                    try {
                        sleep(250);
                        return RendezvousServer.rendezvousWith(node2, rendezvousServerLink, new IdentityTriple("2", new byte[]{}, node2.getSelfLink()), "1")[0];
                    } catch (TimeoutException e) {} //one timeout exception is expected
                }
            });

            IdentityTriple idOf1KnownTo2 = fut2.getOrNull(RendezvousServer.CALLBACK_TIMEOUT*2);
            IdentityTriple idOf2KnownTo1 = fut1.getOrNull(RendezvousServer.CALLBACK_TIMEOUT*2);
            System.out.println("idOf1KnownTo2 = " + idOf1KnownTo2);
            System.out.println("idOf2KnownTo1 = " + idOf2KnownTo1);
            assertNotNull(idOf1KnownTo2);
            assertNotNull(idOf2KnownTo1);

            shortBackAndForthTest(node1, node2, idOf1KnownTo2, idOf2KnownTo1);

            node1.close();
            node2.close();
        }
    }

    private void shortBackAndForthTest(P2LNode node1, P2LNode node2, IdentityTriple idOf1KnownTo2, IdentityTriple idOf2KnownTo1) throws IOException {
        node1.establishConnection(idOf2KnownTo1.link);

        P2LFuture<ReceivedP2LMessage> msgFut = node1.expectMessage(123);

        P2LMessage msgToSend = P2LMessage.with(123);
        msgToSend.encode(-432);
        node2.sendMessage(idOf1KnownTo2.link, msgToSend);

        assertEquals(-432, msgFut.get(1000).nextInt());
    }
}