package jokrey.utilities.network.link2peer.rendevouz;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.NodeCreator;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import javax.naming.NameAlreadyBoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @author jokrey
 */
public class RendezvousServerAttempt1LetsNotTalkAboutIt implements AutoCloseable {
    public static final byte REGISTER = 1;
    public static final byte CALLBACK_REQUEST = 2;
    public static final byte UNREGISTER = 3;

    public static final int CALLBACK_TIMEOUT = 10000;

    public static void main(String[] args) throws IOException {
        RendezvousServerAttempt1LetsNotTalkAboutIt server = new RendezvousServerAttempt1LetsNotTalkAboutIt(args.length == 0? new P2Link.Direct("lmservicesip.ddns.net", 40000) : args.length==1? P2Link.from(args[0]) : new P2Link.Direct(args[0], Integer.parseInt(args[1])));

        System.out.println("running + waiting");
    }


    private final HashMap<String, KnownIdentityTriple> knownIdentitiesByName = new HashMap<>(); //todo clean thread over v.createdAt
//        private final HashMap<P2Link, KnownIdentityTriple> knownIdentitiesByLink = new HashMap<>(); //can be used to prohibit multiple registers from the same ip and or to the same address under different names - or is that allowed?
//        private final HashMap<SocketAddress, KnownIdentityTriple> knownIdentitiesByRegisterer = new HashMap<>();
    private final HashMap<String, List<Pair<P2LConversation, Long>>> waitingCallbackConvos = new HashMap<>(); //todo clean thread over v foreach it.second
    private final P2LNode node;
    @Override public void close() { node.close(); }
    public RendezvousServerAttempt1LetsNotTalkAboutIt(P2Link selfLink) throws IOException {
        node = NodeCreator.create(selfLink, 0); //does not accept peers

        node.registerConversationFor(REGISTER, (convo, m0) -> {
            KnownIdentityTriple id = KnownIdentityTriple.decode(m0, convo.getPeer());

            KnownIdentityTriple previous;
            List<Pair<P2LConversation, Long>> waitingConvos = null;
            synchronized (this) {
                previous = knownIdentitiesByName.get(id.name);
//                if (knownIdentitiesByLink.containsKey(id.address)) throw new IllegalStateException("how exactly is the address known if the name wasn't?");
//                if (knownIdentitiesByRegisterer.containsKey(convo.getPeer())) alreadyKnown = true; //can occur if the same peer tries to register a different name

                if(previous == null) {
                    knownIdentitiesByName.put(id.name, id);
                    waitingConvos = waitingCallbackConvos.remove(id.name);
                    System.out.println("registered:= " + id);
//                    knownIdentitiesByLink.put(id.address, id);
//                    knownIdentitiesByRegisterer.put(id.registerer, id);
                }
            }

            if(previous != null && !previous.equals(id))
                convo.answerClose(convo.encode((byte) -1));
            else
                convo.answerClose(convo.encode((byte) 1));

            if(waitingConvos!=null)
                for(Pair<P2LConversation, Long> waitingConvo : waitingConvos) {
                    if (waitingConvo.r > System.currentTimeMillis()) {
                        System.out.println("answering callback request async, after receiving identity of "+id.name + " - convo is with: "+waitingConvo.getFirst().getPeer());
                        waitingConvo.l.answerClose(id.encode(waitingConvo.l));
                    } else {
                        System.out.println("convo has since timed out, but now received identity of "+id.name + " - convo is with: "+waitingConvo.getFirst().getPeer());
                    }
                }
        });
        node.registerConversationFor(UNREGISTER, (convo, m0) -> {
            KnownIdentityTriple id = KnownIdentityTriple.decode(m0, convo.getPeer());

            synchronized (this) {
                boolean removed = knownIdentitiesByName.computeIfPresent(id.name, (k, current) -> { //done instead of remove so that, if close is called multiple times and someone else registers the same name it does not just dissapear
                    if(id.equals(current))  return null; //delete - also checks that the deleter is the registerer
                    else                    return current;
                }) == null;
                if(removed) {
                    System.out.println(id.name+", unregistered its callback");
//                    knownIdentitiesByLink.remove(id.address);
//                    knownIdentitiesByRegisterer.remove(check.registerer);
                }
            }

            convo.close(); //it is fine that this method 'unregister' is called multiple times if the close message is lost...
        });
        node.registerConversationFor(CALLBACK_REQUEST, (convo, m0) -> {
            convo.pause();

            String requestedName = m0.nextVariableString();
            int timeout = Math.min(CALLBACK_TIMEOUT, m0.nextInt());
            System.out.println("received callback request for " + requestedName+", from="+convo.getPeer());

            KnownIdentityTriple result;
            synchronized (this) {
                result = knownIdentitiesByName.get(requestedName);
                if(result == null) {
                    System.out.println("result(for="+requestedName+") NOT available, callback registered");
                    waitingCallbackConvos.compute(requestedName, (k, pairs) -> {
                        if (pairs == null)
                            pairs = new ArrayList<>();
                        pairs.add(new Pair<>(convo, System.currentTimeMillis() + timeout));
                        return pairs;
                    });
                }
            }

            if(result != null) { //never do io in sync block
                System.out.println("result(for=" + requestedName + ") available, answering in sync");
                convo.answerClose(result.encode(convo));
            }
        });
    }


    //everyone - calls this, each with their own id, but the others names
    //   when the method returns, everybody should know everybody.
    //   as long as everyone calls the method within 10 seconds (CALLBACK TIMEOUT)
    // it is possible and reasonable to deconstruct this method - calling(AND RE-CALLING) requestInfoFor and pushing results to, for example, a ui.
    public static IdentityTriple[] rendezvousWith(P2LNode initiator, P2Link linkToRendezvousServer, IdentityTriple ownId, String... others) throws IOException, NameAlreadyBoundException {
        register(initiator, linkToRendezvousServer, ownId);
        IdentityTriple[] otherIds = new IdentityTriple[others.length];
        for (int i = 0; i < others.length; i++) {
            String remoteName = others[i];
            try {
                otherIds[i] = requestInfoFor(initiator, linkToRendezvousServer, remoteName);
            } catch (TimeoutException e) {
                otherIds[i] = null;
            }
        }
        unregister(initiator, linkToRendezvousServer, ownId);
        return otherIds;
    }
    public static void register(P2LNode initiator, P2Link linkToRendezvousServer, IdentityTriple ownId) throws IOException, NameAlreadyBoundException {
        P2LConversation convo = initiator.convo(REGISTER, linkToRendezvousServer);
        byte result = convo.initExpect(ownId.encode(convo)).nextByte();
        convo.close();
        if(result == -1) throw new NameAlreadyBoundException("The given identity name is already known to the server - try another name");
        else if(result != 1)
            throw new IOException("server returned with unexpected error type");
    }
    public static void unregister(P2LNode initiator, P2Link linkToRendezvousServer, IdentityTriple ownId) throws IOException {
        P2LConversation convo = initiator.convo(UNREGISTER, linkToRendezvousServer);
        convo.initClose(ownId.encode(convo));
    }
    public static IdentityTriple requestInfoFor(P2LNode initiator, P2Link linkToRendezvousServer, String rendezvousRemoteName) throws IOException {
        P2LConversation convo = initiator.convo(CALLBACK_REQUEST, linkToRendezvousServer);
        P2LMessage result = convo.initExpectAfterPause(convo.encode(rendezvousRemoteName, CALLBACK_TIMEOUT), CALLBACK_TIMEOUT);
        convo.close();
        return IdentityTriple.decode(result);
    }


    public static class IdentityTriple {
        public final String name;
        public final byte[] publicKey;
        public final P2Link link;

        public IdentityTriple(String name, byte[] publicKey, P2Link link) {
            this.name = name;
            this.publicKey = publicKey;
            this.link = link;
        }

        MessageEncoder encode(P2LConversation convo) {
            return convo.encode(name, publicKey, link.toBytes());
        }
        static IdentityTriple decode(P2LMessage m) {
            String name = m.nextVariableString();
            byte[] publicKey = m.nextVariable();
            P2Link link = P2Link.from(m.nextVariable());
            return new IdentityTriple(name, publicKey, link);
        }

        @Override public String toString() {
            return "IdentityTriple{" + "name='" + name + '\'' + ", publicKey=" + Arrays.toString(publicKey) + ", address=" + link + '}';
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdentityTriple that = (IdentityTriple) o;
            return Objects.equals(name, that.name) && Arrays.equals(publicKey, that.publicKey) && Objects.equals(link, that.link);
        }
        @Override public int hashCode() {
            return 31 * Objects.hash(name, link) + Arrays.hashCode(publicKey);
        }
    }

    private static class KnownIdentityTriple extends IdentityTriple {
        private final InetSocketAddress registerer;
        private final long createdAt = System.currentTimeMillis(); //todo - can be used to clean up old registrations.
        private KnownIdentityTriple(String name, byte[] publicKey, P2Link link, InetSocketAddress registerer) {
            super(name, publicKey, link);
            this.registerer = registerer;
        }

        public static KnownIdentityTriple decode(P2LMessage m, InetSocketAddress peer) {
            String name = m.nextVariableString();
            byte[] publicKey = m.nextVariable();
            P2Link link = P2Link.from(m.nextVariable());
            return new KnownIdentityTriple(name, publicKey, link, peer);
        }

        @Override public String toString() {
            return "KnownIdentityTriple{" + "name='" + name + '\'' + ", publicKey=" + Arrays.toString(publicKey) + ", address=" + link + ", registerer=" + registerer + ", createdAt=" + createdAt + '}';
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            KnownIdentityTriple that = (KnownIdentityTriple) o;
            return //createdAt == that.createdAt &&
                    Objects.equals(registerer, that.registerer);
        }
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), registerer, createdAt);
        }
    }
}
