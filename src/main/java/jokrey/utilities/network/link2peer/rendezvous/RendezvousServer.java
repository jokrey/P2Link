package jokrey.utilities.network.link2peer.rendezvous;

import jokrey.utilities.command.line.helper.Argument;
import jokrey.utilities.command.line.helper.CommandLoop;
import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.simple.data_structure.BadConcurrentMultiKeyMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;

/**
 * @author jokrey
 */
public class RendezvousServer implements AutoCloseable {
    public static void main(String[] arguments) throws IOException {
        Scanner userIn = new Scanner(System.in);

        String rawInput;
        if(arguments.length > 0) {
            rawInput = arguments[0];
        } else {
            System.out.println("First argument should be the port on which this node should listen for incoming messages\n   preferred however is a full socket address (<ip/dns>:<port>) on which other peers can reach this node");
            System.out.print("Enter own link (<public-ip/dns>:<port>):  \n");
            rawInput = userIn.nextLine();
        }
        String rawLink = rawInput;

        P2Link selfLink = P2Link.from(rawLink);

        System.out.println("Given = " + rawLink);

        if(!selfLink.isDirect())
            throw new IllegalArgumentException("Given link not a direct link - rendezvous requires a link easily reachable by others");

        RendezvousServer server = new RendezvousServer((P2Link.Direct) selfLink);



        server.node.addConnectionEstablishedListener((newAddress, cId) -> System.out.println(newAddress.link + " connected"));
        server.node.addConnectionDroppedListener(disconnected -> System.out.println(disconnected.link +" disconnected"));

        CommandLoop loop = new CommandLoop();
        loop.addCommand("disconnectFromAll", "Closes all connections to peers", Argument.noargs(), args -> server.node.disconnectFromAll(), "clean");
        loop.addCommand("close", "closes this node and kills the process", args -> {
            server.node.close();System.exit(0);
        }, "exit");
        loop.addCommand("printActivePeers", "Prints all active peer links", Argument.noargs(), args -> System.out.println(Arrays.toString(server.node.getEstablishedConnections())),
                "peers");
        loop.addCommand("printSelf", "Prints own nodes link", Argument.noargs(), args -> System.out.println(selfLink),
                "self", "me");
        loop.addCommand("printDebugInformation", "Prints debug information", Argument.noargs(), args -> server.node.printDebugInformation(), "debug");

        loop.run();
    }




    public static final byte DENIED = -1;
    public static final byte SUCCESS = 1;
    public static final int C_REGISTER = 1;
    public static final int C_REQUEST = 2;
    public static final int C_REQUEST_ALL = 3;

    private final P2LNode node;

    private final BadConcurrentMultiKeyMap<String, InetSocketAddress, IdentityTriple> registeredIdentities = new BadConcurrentMultiKeyMap<>();

    public RendezvousServer(P2Link.Direct selfLink) throws IOException {
        node = P2LNode.create(selfLink, 512);

        node.addConnectionDroppedListener(p2LConnection -> {
            registeredIdentities.removeBy2(p2LConnection.address);
        });
        node.registerConversationFor(C_REGISTER, (convo, m0) -> {
            convo.close();
            P2LConnection registeringConversation = node.getConnection(convo.getPeer());
            if(registeringConversation != null) {
                String name = m0.nextVariableString();
                byte[] publicKey = m0.nextVariable();
                P2Link link = registeringConversation.link;
                if(link.isOnlyLocal())
                    link = ((P2Link.Local) link).withRelay(selfLink);
                IdentityTriple decodedIdentityTriple = new IdentityTriple(name, publicKey, link);
                registeredIdentities.put(name, convo.getPeer(), decodedIdentityTriple);
            } else {
                System.out.println(convo.getPeer() + " - tried registering without being connected... That is not possible(then you couldn't use this rendezvous server as a relay node)");
            }
        });
        node.registerConversationFor(C_REQUEST, (convo, m0) -> {
            MessageEncoder replyEncoder = convo.encoder();
            if(!registeredIdentities.containsBy2(convo.getPeer())) { //requester has to be registered here also
                replyEncoder.encode(DENIED);
            } else {
                replyEncoder.encode(SUCCESS);
                String requestedName;
                while ((requestedName = m0.nextVariableString()) != null) {
                    IdentityTriple foundIdentity = registeredIdentities.getBy1(requestedName);
                    if(foundIdentity != null)
                        foundIdentity.encodeInto(replyEncoder, true);
                }
            }
            convo.closeWith(replyEncoder);
        });
        node.registerConversationFor(C_REQUEST_ALL, (convo, no) -> {
            MessageEncoder replyEncoder = convo.encoder();
            IdentityTriple requesterIdentity = registeredIdentities.getBy2(convo.getPeer());
            if(requesterIdentity == null) { //requester has to be registered here also
                replyEncoder.encode(DENIED);
            } else {
                replyEncoder.encode(SUCCESS);
                for(IdentityTriple identityTriple : registeredIdentities.values(new IdentityTriple[0])) {
                    if (!identityTriple.equals(requesterIdentity))
                        identityTriple.encodeInto(replyEncoder, true);
                    if(replyEncoder.size >= convo.getMaxPayloadSizePerPackage()) break; //already requires multiple packages... Stop here...
                }
            }
            convo.closeWith(replyEncoder);
        });
    }

    @Override public void close() { node.close(); }
    //todo - do protocols async where possible

    /*
    SIMPLY DISCONNECT TO UNREGISTER
     */

    public static P2LFuture<Boolean> register(P2LNode node, P2Link.Direct rendezvousServerLink, IdentityTriple selfIdentity) throws IOException {
        return node.establishConnection(rendezvousServerLink).andThen(alwaysTrue -> {
            P2LConversation convo = node.convo(C_REGISTER, rendezvousServerLink);
            return convo.initCloseAsync(selfIdentity.encodeInto(convo.encoder(), false));
        });
    }

    /** Node has to be previously registered with {@link #register(P2LNode, P2Link.Direct, IdentityTriple)} */
    public static IdentityTriple[] request(P2LNode node, P2Link.Direct rendezvousServerLink, String... names) throws IOException {
        P2LConversation convo = node.convo(C_REQUEST, rendezvousServerLink);
        MessageEncoder requestEncoder = convo.encoder();
        for (String name : names)
            requestEncoder.encodeVariableString(name);

        ReceivedP2LMessage reply = convo.initExpectClose(requestEncoder);

        ArrayList<IdentityTriple> foundIdentities = new ArrayList<>();
        IdentityTriple foundIdentity;
        while(reply.hasAnyMore() && reply.nextByte() == SUCCESS && (foundIdentity = IdentityTriple.decodeNextOrNull(reply)) != null)
            foundIdentities.add(foundIdentity);
        return foundIdentities.toArray(new IdentityTriple[0]);
    }

    /** Node has to be previously registered with {@link #register(P2LNode, P2Link.Direct, IdentityTriple)} */
    public static IdentityTriple[] requestAsManyAsPossible(P2LNode node, P2Link.Direct rendezvousServerLink) throws IOException {
        P2LConversation convo = node.convo(C_REQUEST_ALL, rendezvousServerLink);

        ReceivedP2LMessage reply = convo.initExpectClose();

        ArrayList<IdentityTriple> foundIdentities = new ArrayList<>();
        IdentityTriple foundIdentity;
        while(reply.hasAnyMore() && reply.nextByte() == SUCCESS && (foundIdentity = IdentityTriple.decodeNextOrNull(reply)) != null)
            foundIdentities.add(foundIdentity);
        return foundIdentities.toArray(new IdentityTriple[0]);
    }


    public static IdentityTriple[] rendezvousWith(P2LNode node, P2Link.Direct rendezvousServerLink, IdentityTriple selfIdentity, int timeout, String... names) throws IOException {
        long startedAt = System.currentTimeMillis();
        register(node, rendezvousServerLink, selfIdentity).waitForIt(timeout);

        ArrayList<IdentityTriple> connectedPeers = new ArrayList<>();
        ArrayList<String> remainingNames = new ArrayList<>(Arrays.asList(names));
        main: while(connectedPeers.size() < names.length) {
            int elapsed = (int) (System.currentTimeMillis() - startedAt);
            int remaining = timeout - elapsed;
            if(remaining <= 0) break;

            IdentityTriple[] newlyFound = request(node, rendezvousServerLink, remainingNames.toArray(new String[0]));
            for(IdentityTriple it : newlyFound) {
                elapsed = (int) (System.currentTimeMillis() - startedAt);
                remaining = timeout - elapsed;
                if(remaining <= 0) break main;

                boolean success = node.establishConnection(it.link).get(remaining);
                if(success) {
                    connectedPeers.add(it);
                    remainingNames.remove(it.name);
                }
            }
            sleep(100);
        }
        node.disconnectFrom(rendezvousServerLink);

        return connectedPeers.toArray(new IdentityTriple[0]);
    }
}
