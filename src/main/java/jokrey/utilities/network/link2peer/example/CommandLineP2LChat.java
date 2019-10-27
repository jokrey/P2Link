package jokrey.utilities.network.link2peer.example;

import jokrey.utilities.command.line.helper.Argument;
import jokrey.utilities.command.line.helper.CommandLoop;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * @author jokrey
 */
public class CommandLineP2LChat {
    public static void main(String[] arguments) throws IOException {
        Scanner userIn = new Scanner(System.in);

        P2Link selfLink;
        if(arguments.length > 0) {
            selfLink = new P2Link(arguments[0]);
        } else {
            System.out.println("First argument should be the public link of this node(of the form '[ip/dns]:[port]'),\n   if no public facing ip is known the port is sufficient.");
            System.out.print("Enter link / port:  ");
            selfLink = new P2Link(userIn.nextLine());
        }

        System.out.println("Given selfLink = " + selfLink);

        P2LNode node = P2LNode.create(selfLink);

        node.addIndividualMessageListener(message -> System.out.println(node.getSelfLink()+" received message(from "+message.sender+"):\n"+message.asString()));
        node.addBroadcastListener(message -> System.out.println(node.getSelfLink()+" received broadcast(from "+message.sender+"):\n"+message.asString()));

        CommandLoop loop = new CommandLoop();
        loop.addCommand("connectTo", "Connects to peer at link(args[0])", Argument.with(String.class), args -> {
            P2Link connectTo = new P2Link(args[0].getRaw());
            boolean success = node.connectToPeer(connectTo);
            if(success)
                System.out.println("connected to "+connectTo);
        },"connect");
        loop.addCommand("garnerConnectionsFrom", "Recursively garners n(args[1]) connections from setup link(args[0])", Argument.with(String.class, Integer.class), args -> {
            P2Link garnerFrom = new P2Link(args[0].getRaw());
            Integer limit = args[1].get();
            List<P2Link> newlyConnected = node.recursiveGarnerConnections(limit, garnerFrom);
            System.out.println("Newly connected to: " + newlyConnected);
        },"garner");
        loop.addCommand("disconnectFromAll", "Closes all connections to peers", Argument.noargs(), args -> node.disconnect(),
                "disconnect", "close");
        loop.addCommand("printActivePeers", "Prints all active peer links", Argument.noargs(), args -> System.out.println(node.getActivePeerLinks()),
                "peers");
        loop.addCommand("printSelfLink", "Prints own nodes link", Argument.noargs(), args -> System.out.println(node.getSelfLink()),
                "self", "selfLink");
        loop.addCommand("sendBroadcast", "Sends a string(args[0]) as a broadcast", Argument.with(String.class), args -> {
            node.sendBroadcast(P2LMessage.createSendMessage(0, args[0].getRaw()));
        }, "broadcast", "brd");
        loop.addCommand("sendIndividualMessage", "Sends a string(args[1]) as an individual message to an active peer link(args[0])", Argument.with(String.class, String.class), args -> {
            node.sendIndividualMessageTo(new P2Link(args[0].getRaw()), P2LMessage.createSendMessage(0, args[1].getRaw()));
        }, "sendMessage", "send");

        loop.run();
    }

    private static int portFrom(String arg) {
        try {
            int port = Integer.parseInt(arg);
            if(port < 0 || port > Short.MAX_VALUE*2)
                throw new NumberFormatException();
            return port;
        } catch (NumberFormatException e) {
            System.out.println("First argument should be the port. Argument("+arg+") is not a valid port number.");
            System.exit(1);
            return -1;
        }
    }
}
