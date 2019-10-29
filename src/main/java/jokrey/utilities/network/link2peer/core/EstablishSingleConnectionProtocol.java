package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

//TODO: problem analysis:
//TODO:   on retries, which this peer does not know about, it is very possible that the wrong thread consumes a message request from a queue...
//TODO:   for example the receipt here is consumed, but it is actually from a different retry attempt
//TODO:   this can occur for all message expects...
//TODO: ONLY ONE asAnswerer for each protocol+from combination...

//TODO: Other issues:
//      when receipt is dropped message is send again - is handled again or not...
//      initiator and answerer go out of sync
class EstablishSingleConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

//    public static void asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
//        System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" beginning request");
//        parent.sendInternalMessageBlocking(P2LMessage.createSendMessage(SL_PEER_CONNECTION_REQUEST), to, 3, 500);
//        System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" request received by peer");
//        P2LMessage message = parent.expectInternalMessage(to, R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST).get(3000);
//
//        byte[] verifyNonce = message.asBytes();
//        System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" init side verifyNonce(received) = " + Arrays.toString(verifyNonce));
//        if (verifyNonce.length == 1) {
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" other says connected");
//            parent.graduateToEstablishedConnection(to);
//        } else if(verifyNonce.length != 0) {
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" sending nonce back");
//            parent.sendInternalMessageBlocking(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, verifyNonce), to, 3, 500);
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" send nonce back");
//            parent.expectInternalMessage(to, R_CONNECTION_ESTABLISHED).get(3000);
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")init"+" got established verification");
//            parent.graduateToEstablishedConnection(to);
//        }
//    }
//
//    public static void asReceiver(P2LNodeInternal parent, InetSocketAddress from, P2LMessage initialRequestMessage) throws IOException {
//        if(parent.connectionLimitReached()) {
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" refused connection request by "+from+" - max peers");
//            parent.sendInternalMessageBlocking(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST), from, 3, 500); //do not retry refusal
//            return;
//        }
//        if(parent.isConnectedTo(from)) {
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" refused connection request by "+from+" - already connected");
//            parent.sendInternalMessageBlocking(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, new byte[1]), from, 3, 500); //do not retry refusal
//            return;
//        }
//
//        // send a nonce to other peer
//        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
//        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
//        byte[] nonce = new byte[16];
//        secureRandom.nextBytes(nonce);
//        //next line blocking, due to the following problem: if the packet is lost, the initiator will retry the connection:
//        //     sending a new connection-request + receiving a verify nonce request WITH A DIFFERENT NONCE - however, since this came first, we will receive the nonce here...
//        System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" answer side nonce(sending) = " + Arrays.toString(nonce));
//        parent.sendInternalMessageBlocking(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, nonce), from, 3, 500);
//        System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" answer side nonce(send done)");
//
//        byte[] verifyNonce = parent.expectInternalMessage(from, R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER).get(6000).asBytes();
//        System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" answer side verifyNonce(received) = " + Arrays.toString(verifyNonce));
//        if(Arrays.equals(nonce, verifyNonce)) {
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" accepted connection from = " + from);
//            parent.graduateToEstablishedConnection(from);
//            parent.sendInternalMessageBlocking(P2LMessage.createSendMessage(R_CONNECTION_ESTABLISHED), from, 3, 500);
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")"+" accepted connection from(send established) = " + from);
//        } else {
//            System.err.println(parent.getPort()+"("+Thread.currentThread().getId()+")" + " refused connection request by " + from + " - wrong nonce");
//        }
//    }
    public static boolean asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.asInitiator(parent, link, outgoing);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }

        boolean success = parent.tryReceive(3, 2000, () -> {
            if(parent.isConnectedTo(to)) return new P2LFuture<>(false);
            int conversationId = parent.createUniqueConversationId();
            parent.sendInternalMessage(P2LMessage.createSendMessage(SL_PEER_CONNECTION_REQUEST, conversationId), to);
            System.err.println(parent.getPort()+":with("+to+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" send connection request");
            return parent.expectInternalMessage(to, R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId).combine(message -> {
                byte[] verifyNonce = message.asBytes();
                System.err.println(parent.getPort()+":with("+to+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" init side verifyNonce = " + Arrays.toString(verifyNonce));
                if(verifyNonce.length==0) return new P2LFuture<>(false);
                if(verifyNonce.length==1) {
                    System.err.println(parent.getPort()+":with("+to+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" other says connected");
                    return new P2LFuture<>(true);
                }
                try {
                    parent.sendInternalMessage(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId, verifyNonce), to);
                    System.err.println(parent.getPort()+":with("+to+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" send answer - expecting R_CONNECTION ESTABLISHED");
                    return parent.expectInternalMessage(to, R_CONNECTION_ESTABLISHED, conversationId).toBooleanFuture(m->true);
                } catch (IOException e) {
                    System.err.println(parent.getPort()+":with("+to+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" exception: "+e.getMessage());
                    e.printStackTrace();
                    return new P2LFuture<>(null);//causes retry!!
                }
            });
        });
        System.err.println(parent.getPort()+":with("+to+")[outer]:"+"("+Thread.currentThread().getId()+")"+" success: "+success);

        if(success)
            parent.graduateToEstablishedConnection(to);
        return success;
    }

    public static void asReceiver(P2LNodeInternal parent, InetSocketAddress from, P2LMessage initialRequestMessage) throws IOException {
        int conversationId = initialRequestMessage.conversationId;
        if(parent.connectionLimitReached()) {
            parent.sendInternalMessage(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId), from); //do not retry refusal
            System.err.println(parent.getPort()+":with("+from+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" refused connection request by "+from+" - max peers");
            return;
        }
        if(parent.isConnectedTo(from)) {
            parent.sendInternalMessage(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, new byte[1]), from); //do not retry refusal
            System.err.println(parent.getPort()+":with("+from+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" refused connection request by "+from+" - already connected");
            return;
        }
//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.asInitiator(parent, link, outgoing);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }


        // send a nonce to other peer
        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
        byte[] nonce = new byte[16];
        secureRandom.nextBytes(nonce);
        System.err.println(parent.getPort()+":with("+from+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" answer side nonce = " + Arrays.toString(nonce));
        //next line blocking, due to the following problem: if the packet is lost, the initiator will retry the connection:
        //     sending a new connection-request + receiving a verify nonce request WITH A DIFFERENT NONCE - however, since this came first, we will receive the nonce here...
        parent.sendInternalMessage(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, nonce), from);



        //TODO: problem analysis:
        //TODO:   on retries, which this side of the protocol does not know about, it is very possible that the wrong thread consumes a message request from a queue...
        //TODO:   for example the receipt here is consumed, but it is actually from a different retry attempt
        //TODO:   this can occur for all message expects...
        byte[] verifyNonce = parent.expectInternalMessage(from, R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId).get(10000).asBytes();
        System.err.println(parent.getPort()+":with("+from+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" answer side verifyNonce = " + Arrays.toString(verifyNonce));
        if(Arrays.equals(nonce, verifyNonce)) {
            System.err.println(parent.getPort()+":with("+from+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" accepted connection from = " + from);
            parent.graduateToEstablishedConnection(from);
            parent.sendInternalMessage(P2LMessage.createSendMessage(R_CONNECTION_ESTABLISHED, conversationId), from);
        } else {

            System.err.println(parent.getPort()+":with("+from+")["+conversationId+"]:"+"("+Thread.currentThread().getId()+")"+" refused connection request by " + from + " - wrong nonce");
        }
    }
}


//TODO::  question: could it be good to add a thread id to messages? so that only a specific thread could consume a message?
//TODO:       thread id's are shared(due to the worker pool), but only for
//TODO:: abstraction in 'conversation id'