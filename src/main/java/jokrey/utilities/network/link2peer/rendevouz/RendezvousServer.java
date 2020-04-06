package jokrey.utilities.network.link2peer.rendevouz;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.NodeCreator;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import javax.naming.NameAlreadyBoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jokrey
 */
public class RendezvousServer implements AutoCloseable {
    public static final byte REGISTER = 1;

    public static final int CALLBACK_TIMEOUT = 10000;

    public static void main(String[] args) throws IOException {
        P2Link selfLink = args.length == 0? new P2Link.Direct("lmservicesip.ddns.net", 40000) : args.length==1? P2Link.from(args[0]) : new P2Link.Direct(args[0], Integer.parseInt(args[1]));
        RendezvousServer server = new RendezvousServer(selfLink);

        System.out.println("selfLink = " + selfLink);
        System.out.println("It is important to have a public link as a self link (i.e. one accessible from anywhere)\n    because we use it as a relay link as well.");
        System.out.println("running + waiting");
    }


    //todo -
    //  there is a possible denial of service attack, by filling up the ram:
    //  register A, register B
    //  do not answer with close message from B
    //    server will assume that B will retry and keep the registration open (forever)
    //  (this can also be considered a memory leak)
    private final ConcurrentHashMap<Set<String>, Pair<List<IdentityTriple>, AtomicInteger>> currentRendezvousRegistrations = new ConcurrentHashMap<>();
    private final P2LNode node;
    @Override public void close() { node.close(); }
    public RendezvousServer(P2Link selfLink) throws IOException {
        node = NodeCreator.create(selfLink, 0); //does not accept peers

        node.registerConversationFor(REGISTER, (convo, m0) -> {
            long startTime = System.currentTimeMillis();

            convo.pause();

            IdentityTriple originalId = IdentityTriple.decodeNext(m0);
            IdentityTriple id = //originalId.address.isOnlyLocal() ? //todo dododo
                   // originalId.withRelayLinkInAddress(selfLink) :
                    originalId;

            ArrayList<String> requestedContacts = new ArrayList<>();
            String contactName;
            while((contactName = m0.nextVariableString()) != null)
                requestedContacts.add(contactName);
            requestedContacts.add(id.name);
            Set<String> rendezvousParties = new HashSet<>(requestedContacts);
            requestedContacts.remove(id.name);


            AtomicBoolean nameDenied = new AtomicBoolean(false);
            AtomicReference<Pair<List<IdentityTriple>, AtomicInteger>> knownsRef = new AtomicReference<>();
            currentRendezvousRegistrations.compute(rendezvousParties, (k, v) -> {
                if(v==null) {
                    CopyOnWriteArrayList<IdentityTriple> list = new CopyOnWriteArrayList<>();
                    list.add(id);
                    Pair<List<IdentityTriple>, AtomicInteger> p = new Pair<>(list, new AtomicInteger(0));
                    knownsRef.set(p);
                    return p;
                } else {
                    knownsRef.set(v);

                    boolean wasPreviouslyRegistered = false;
                    for(IdentityTriple kit : v.l) {
                        if(kit.name.equals(id.name)) {
                            wasPreviouslyRegistered = true;
                            if(!kit.equals(id))
                                nameDenied.set(true);
                            break;
                        }
                    }
                    if(! wasPreviouslyRegistered)
                        v.l.add(id);
                    if(v.l.size() == k.size()) {
                        SyncHelp.notifyAll(v);
                        return null;
                    } else
                        return v;
                }
            });
            Pair<List<IdentityTriple>, AtomicInteger> knowns = knownsRef.get();
            if(nameDenied.get()) {
                convo.answerClose(convo.encode((byte) -1));
                System.out.println(convo.getPeer()+" - denied, attempted to use name \""+id.name+"\", but name is already occupied for rendezvous");
            } else {
                System.out.println(convo.getPeer()+"("+id.name+") has registered request for "+requestedContacts);

                int elapsed = (int) (System.currentTimeMillis() - startTime);
                if (elapsed >= CALLBACK_TIMEOUT ||
                        ! SyncHelp.waitUntil(knowns, () -> knowns.l.size() == requestedContacts.size() + 1, CALLBACK_TIMEOUT - elapsed)) {
                    unregister(id, rendezvousParties);

                    System.out.println(convo.getPeer()+"("+id.name+") has timed out with its request for: "+requestedContacts);
                    return;
                }

                MessageEncoder encoder = convo.encode((byte) 1);
                for(IdentityTriple knownIdentity : knowns.l)
                    if(!id.name.equals(knownIdentity.name))
                        knownIdentity.encodeInto(encoder);

                System.out.println(convo.getPeer()+"("+id.name+") has all information and is sending data "+requestedContacts);
                try {
                    convo.answerClose(encoder);
                } catch (TimeoutException e) {
                    unregister(id, rendezvousParties);
                    System.out.println(convo.getPeer()+"("+id.name+") could not be answered, despite all information being available - timeout in answerClose [remote should retry]");
                    return;//this is fine. The remote can retry and will find that the registrations are still there.
                }

                knowns.r.getAndIncrement();
                if(knowns.r.get() == requestedContacts.size()+1)
                    currentRendezvousRegistrations.remove(rendezvousParties);
            }
        });
    }

    private void unregister(IdentityTriple id, Set<String> rendezvousParties) {
        currentRendezvousRegistrations.compute(rendezvousParties, (k, v) -> {
            if(v!=null) {
                v.l.remove(id);
                if (v.l.isEmpty())
                    return null;
            }
            return v;
        });
    }


    //everyone - calls this, each with their own id, but the others names
    //   when the method returns, everybody should know everybody.
    //   as long as everyone calls the method within 10 seconds (CALLBACK TIMEOUT)
    // it is possible and reasonable to deconstruct this method - calling(AND RE-CALLING) requestInfoFor and pushing results to, for example, a ui.
    public static IdentityTriple[] rendezvousWith(P2LNode initiator, P2Link linkToRendezvousServer, IdentityTriple ownId, String... others) throws IOException, NameAlreadyBoundException {
        P2LConversation convo = initiator.convo(REGISTER, linkToRendezvousServer);
        MessageEncoder encoder = ownId.encodeInto(convo.encoder());
        for(String other: others)
            encoder.encodeVariableString(other);

        P2LMessage m = convo.initExpectAfterPause(encoder, CALLBACK_TIMEOUT);
        convo.close();
        byte result = m.nextByte();
        if(result == 1) {
            IdentityTriple[] otherIds = new IdentityTriple[others.length];
            for (int i = 0; i < others.length; i++)
                otherIds[i] = IdentityTriple.decodeNext(m);
            return otherIds;
        } else {
            throw new NameAlreadyBoundException();
        }
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

        MessageEncoder encodeInto(MessageEncoder encoder) {
            encoder.encodeVariableString(name);
            encoder.encodeVariable(publicKey);
            encoder.encodeVariable(link.toBytes());
            return encoder;
        }
        static IdentityTriple decodeNext(P2LMessage m) {
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
}
