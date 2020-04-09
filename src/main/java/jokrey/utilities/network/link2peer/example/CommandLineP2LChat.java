package jokrey.utilities.network.link2peer.example;

import jokrey.utilities.command.line.helper.Argument;
import jokrey.utilities.command.line.helper.CommandLoop;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * @author jokrey
 */
public class CommandLineP2LChat {
    public static void main(String[] arguments) throws IOException {
        Scanner userIn = new Scanner(System.in);

        String rawInput;
        if(arguments.length > 0) {
            rawInput = arguments[0];
        } else {
            System.out.println("First argument should be the port on which this node should listen for incoming messages\n   preferred however is a full socket address (<ip/dns>:<port>) on which other peers can reach this node");
            System.out.print("Enter own link (type \"format\" to see a description of the link format):  \n");
            while((rawInput = userIn.nextLine()).equalsIgnoreCase("format")) {
                System.out.println("Link Format:\n" +
                        "  direct link:\n" +
                        "     <ip/dns>:<port>\n" +
                        "  relayed link [NO USE HERE - CAN BE USED TO CONNECT]:\n" +
                        "     <name>[<direct link of relay server>]\n" +
                        "  local link:\n" +
                        "     <name>[local=<port>]");
                System.out.print("Enter own link (type \"format\" to see a description of the link format):  \n");
            }
        }
        String rawLink = rawInput;

        System.out.println("Given = " + rawLink);

        P2LNode node = P2LNode.create(P2Link.from(rawLink));

        node.addMessageListener(message -> System.out.println(rawLink+" received message(from "+message.sender+"):\n"+message.asString()));
        node.addBroadcastListener(message -> System.out.println(rawLink+" received broadcast(from "+message.source+"):\n"+message.asString()));
        node.addConnectionEstablishedListener((newAddress, cId) -> System.out.println(rawLink+" established a new connection to "+newAddress.link));
        node.addConnectionDroppedListener(disconnected -> System.out.println(rawLink+" connection to "+disconnected.link +" disconnected"));

        CommandLoop loop = new CommandLoop();
        loop.addCommand("connectTo", "Connects to peer at link(args[0]) of the form [ip/dns]:[port]", Argument.with(String.class), args -> {
            P2Link connectTo = P2Link.from(args[0].getRaw());
            boolean success = node.establishConnection(connectTo).get();
            if(success)
                System.out.println("connected to "+connectTo);
        },"connect");
        loop.addCommand("garnerConnectionsFrom", "Recursively garners n(args[1]) connections from setup link(args[0])", Argument.with(String.class, Integer.class), args -> {
            P2Link garnerFrom = P2Link.from(args[0].getRaw());
            Integer limit = args[1].get();
            List<P2Link> newlyConnected = node.recursiveGarnerConnections(limit, garnerFrom).get(5000);
            System.out.println("Newly connected to: " + newlyConnected);
        },"garner");
        loop.addCommand("requestFrom", "Request known connections from setup link(args[0]) - returned links can be connected to", Argument.with(String.class), args -> {
            P2Link requestFrom = P2Link.from(args[0].getRaw());
            try {
                List<P2Link> links = node.queryKnownLinksOf(requestFrom).get(2500);
                System.out.println("links = " + links);
            } catch (TimeoutException e) {
                System.err.println(e.getClass().getName() + " error requesting "+e.getMessage());
                e.printStackTrace();
            }
        },"request");
        loop.addCommand("disconnectFromAll", "Closes all connections to peers", Argument.noargs(), args -> node.disconnectFromAll());
        loop.addCommand("close", "closes this node and kills the process", args -> {
            node.close();System.exit(0);
        }, "exit");
        loop.addCommand("printActivePeers", "Prints all active peer links", Argument.noargs(), args -> System.out.println(Arrays.toString(node.getEstablishedConnections())),
                "peers");
        loop.addCommand("printSelf", "Prints own nodes link", Argument.noargs(), args -> System.out.println(node.getSelfLink()),
                "self", "me");
        loop.addCommand("toggleDebugPrintouts", "All Debug Messages Will Be Printed", Argument.noargs(), args -> DebugStats.MSG_PRINTS_ACTIVE = !DebugStats.MSG_PRINTS_ACTIVE,
                "printAll");
        loop.addCommand("sendBroadcast", "Sends a string(args[0]) as a broadcast", Argument.with(String.class), args ->
                node.sendBroadcastWithReceipts(P2LMessage.with(0, args[1].getRaw().getBytes(StandardCharsets.UTF_8))), "broadcast", "brd");
        loop.addCommand("sendIndividualMessage", "Sends a string(args[1]) as an individual message to an active peer link(args[0])", Argument.with(String.class, String.class), args -> {
            P2Link to = P2Link.from(args[0].getRaw()); //unresolved, maybe
            try {
                P2LFuture<Boolean> successFut = node.sendMessageWithReceipt(to, P2LMessage.with(0, args[1].getRaw().getBytes(StandardCharsets.UTF_8)));
                Boolean success = successFut.getOrNull(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT * 2);
                System.out.println(success!=null&&success?"successfully send":"no response (peer did not receive)");
            } catch (IOException e) {
                System.err.println("error sending message "+e.getMessage());
                e.printStackTrace();
            }
        }, "sendMessage", "send");
        loop.addCommand("printDebugInformation", "Prints debug information", Argument.noargs(), args -> node.printDebugInformation(), "debug");

        loop.run();
    }
}
