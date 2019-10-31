package jokrey.utilities.network.link2peer.example;

import jokrey.utilities.command.line.helper.Argument;
import jokrey.utilities.command.line.helper.CommandLoop;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Scanner;

/**
 * @author jokrey
 */
public class CommandLineP2LChat {
    public static void main(String[] arguments) throws IOException {
        Scanner userIn = new Scanner(System.in);

        int port;
        if(arguments.length > 0) {
            port = Integer.parseInt(arguments[0]);
        } else {
            System.out.println("First argument should be the port on which this node should listen for incoming messages");
            System.out.print("Enter port:  ");
            port = Integer.parseInt(userIn.nextLine());
        }

        System.out.println("Given port = " + port);

        P2LNode node = P2LNode.create(port);

        node.addMessageListener(message -> System.out.println(port+" received message(from "+message.sender+"):\n"+message.asString()));
        node.addBroadcastListener(message -> System.out.println(port+" received broadcast(from "+message.sender+"):\n"+message.asString()));
        node.addConnectionEstablishedListener(newAddress -> System.out.println(port+" established a new connection to "+newAddress));
        node.addConnectionDisconnectedListener(disconnected -> System.out.println(port+" connection to "+disconnected +" disconnected"));

        CommandLoop loop = new CommandLoop();
        loop.addCommand("connectTo", "Connects to peer at link(args[0]) of the form [ip/dns]:[port]", Argument.with(String.class), args -> {
            SocketAddress connectTo = WhoAmIProtocol.fromString(args[0].getRaw());
            boolean success = node.establishConnection(connectTo).get();
            if(success)
                System.out.println("connected to "+connectTo);
        },"connect");
        loop.addCommand("garnerConnectionsFrom", "Recursively garners n(args[1]) connections from setup link(args[0])", Argument.with(String.class, Integer.class), args -> {
            SocketAddress garnerFrom = WhoAmIProtocol.fromString(args[0].getRaw());
            Integer limit = args[1].get();
            List<SocketAddress> newlyConnected = node.recursiveGarnerConnections(limit, garnerFrom);
            System.out.println("Newly connected to: " + newlyConnected);
        },"garner");
        loop.addCommand("disconnectFromAll", "Closes all connections to peers", Argument.noargs(), args -> node.disconnectFromAll());
        loop.addCommand("close", "closes this node and kills the process", args -> {
            node.close();System.exit(0);
        }, "exit");
        loop.addCommand("printActivePeers", "Prints all active peer links", Argument.noargs(), args -> System.out.println(node.getEstablishedConnections()),
                "peers");
        loop.addCommand("printOwnPort", "Prints own nodes link", Argument.noargs(), args -> System.out.println(node.getPort()),
                "self", "port");
        loop.addCommand("sendBroadcast", "Sends a string(args[0]) as a broadcast", Argument.with(String.class), args ->
                node.sendBroadcastWithReceipts(P2LMessage.Factory.createBroadcast("iam"+port, 0, args[0].getRaw())), "broadcast", "brd");
        loop.addCommand("sendIndividualMessage", "Sends a string(args[1]) as an individual message to an active peer link(args[0])", Argument.with(String.class, String.class), args -> {
            SocketAddress to = WhoAmIProtocol.fromString(args[0].getRaw());
            try {
                P2LFuture<Boolean> success = node.sendMessageWithReceipt(to, P2LMessage.Factory.createSendMessage(0, args[1].getRaw()));
                System.out.println(success.get(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT)?"successfully send":"no response (peer did not receive)");
            } catch (IOException e) {
                System.err.println("error sending message "+e.getMessage());
                e.printStackTrace();
            }
        }, "sendMessage", "send");
        loop.addCommand("printDebugInformation", "Prints debug information", Argument.noargs(), args -> node.printDebugInformation(), "debug");

        loop.run();
    }
}
