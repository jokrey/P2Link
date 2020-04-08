package jokrey.utilities.network.link2peer;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.util.Hash;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

public class P2LBroadcastMessage extends P2LMessage {
    public final P2Link source;
    public P2LBroadcastMessage(P2LMessageHeader header, P2Link source, Hash contentHash, byte[] raw, int payloadLength) {
        super(header, contentHash, raw, payloadLength);
        this.source = source;
        if(header.isStepPresent()) throw new IllegalArgumentException("broadcast cannot be part of p2lconversation");
        if(header.isStreamPart()) throw new IllegalArgumentException("broadcast cannot be part of stream");
        if(header.isLongPart()) throw new IllegalArgumentException("broadcast cannot be part of broken up message");
//        if(header.isReceipt()) throw new IllegalArgumentException("broadcast cannot be receipt");  - WHY NOT, it is fine
//        if(header.requestReceipt()) throw new IllegalArgumentException("broadcast cannot request receipt");  - WHY NOT, it is fine
    }

    public static P2LBroadcastMessage from(P2Link source, P2LMessage message) {
        return new P2LBroadcastMessage(message.header, source, null, message.content, message.getPayloadLength());
    }


    //the broadcast algorithm requires the message to be packed before sending - this is required to allow header information such as type and expiresAfter to differ from the carrying message
    //    additionally the sender needs to be explicitly stored - as it is omitted/automatically determined in normal messages
    public void packInto(MessageEncoder encoder) {
        int highestByteWritten = header.writeTo(encoder.content, encoder.offset);
//        System.out.println("highestByteWritten = " + highestByteWritten);
//        System.out.println("encoder after skip = " + encoder);
        encoder.skip(highestByteWritten - encoder.offset);
//        System.out.println("encoder after skip = " + encoder);
        encoder.encodeVariable(source.toBytes());
        encoder.encodeVariable(asBytes()); //todo - no need to create array in 'asBytes', could just copy over
//        System.out.println("encoder = " + encoder);
//        System.out.println("header = " + header);
//        System.out.println("header.size = " + header.getSize());
//        System.out.println();
    }
    public static P2LBroadcastMessage unpackFrom(MessageEncoder containerMessage) {
//        System.out.println("containerMessage = " + containerMessage);
//        System.out.println("containerMessage.offset = " + containerMessage.offset);
        P2LMessageHeader header = P2LMessageHeader.from(containerMessage.content, containerMessage.offset);
        containerMessage.skip(header.getSize());

//        System.out.println("header = " + header);
//        System.out.println("header.getSize() = " + header.getSize());
//        System.out.println("containerMessage 2 = " + containerMessage.toString());

        P2Link source = P2Link.from(containerMessage.nextVariable());
        return header.generateBroadcastMessage(source, containerMessage.nextVariable()); //todo should be able to copy without decoding entire variable
    }
}
