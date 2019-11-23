package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author jokrey
 */
public class P2LFragmentOutputStreamImplV1 extends P2LFragmentOutputStream {
    int packageSize;
    protected P2LFragmentOutputStreamImplV1(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId, TransparentBytesStorage source) {
        super(parent, to, con, type, conversationId, source);
        int headerSize = new StreamPartHeader(null, type, conversationId, 0, false, false).getSize();
        packageSize = con==null?1024:con.remoteBufferSize - headerSize;
    }

    boolean allSend = false;
    boolean allReceived = false;

    int batch_size = 64;
    int batch_delay_ms = con==null?500:con.avRTT/2;

    @Override public void sendSource(int timeout_ms) throws InterruptedException, IOException {
        long totalContent = source.contentSize();

        long numSend = 0;
        while(numSend <= totalContent) {
            enqueueInBatch(source.subStorage(numSend, numSend+=packageSize));
            sendBatch();
        }
        allSend=true;
        sendBatch();

        SyncHelp.waitUntil(this, () -> allReceived, timeout_ms);
    }


    @Override public void receivedReceipt(P2LMessage rawReceipt) {
        P2LOrderedStreamReceipt receipt = P2LOrderedStreamReceipt.decode(rawReceipt);
        if(receipt.latestReceived >= source.contentSize()) {
            allReceived = true;
            SyncHelp.notify(this);
        } else {
            for(int missingOffset:receipt.missingParts)
                enqueueInBatch(source.subStorage(missingOffset, missingOffset+packageSize));
        }
    }

    private ConcurrentLinkedQueue<SubBytesStorage> batch = new ConcurrentLinkedQueue<>();
    private boolean batchSizeReached() { return batch.size() >= batch_size; }

    private void enqueueInBatch(SubBytesStorage toSend) {
        batch.offer(toSend);
    }

    private long lastBatchSentAt = System.currentTimeMillis();
    private void sendBatch() throws IOException, InterruptedException {
        if(batchSizeReached() || (allSend && !batch.isEmpty())) {
            int numPackagesToSend = Math.min(batch_size, batch.size());
            Iterator<SubBytesStorage> packageQueue = batch.iterator();
            while(numPackagesToSend > 0) {
                SubBytesStorage packageContent = packageQueue.next();
                packageQueue.remove();//will be re-enqueued by received receipt, if the receiver realizes that it is missing
//                System.out.println("packageContent.getContent() = " + Arrays.toString(packageContent.getContent()));
//                System.out.println("packageContent.contentSize() = " + packageContent.contentSize());
                StreamPartHeader header = new StreamPartHeader(null, type, conversationId, (int) packageContent.start, false, false);
                byte[] content = header.generateRaw(packageContent.getContent());
                P2LMessage message = new P2LMessage(
                        header, null, content, (int) packageContent.contentSize());

                long elapsedInBatchWindow = System.currentTimeMillis() - lastBatchSentAt;
                long msLeftInCurrentBatchWindow = batch_delay_ms - elapsedInBatchWindow;
                if(msLeftInCurrentBatchWindow > 0) {
                    long msLeftToSendCurrentPackage = msLeftInCurrentBatchWindow / numPackagesToSend; //send at the end of the package window.. - just like we are supposed to have send the batch batch-delay after the last batch was send..
                    if (msLeftToSendCurrentPackage > 1)//todo - this may be wrong, but a spin lock feels even more wrong....
                        Thread.sleep(msLeftToSendCurrentPackage);
                }

                parent.sendInternalMessage(message, to);

                numPackagesToSend--;
            }
            lastBatchSentAt = System.currentTimeMillis();
        }
    }

    @Override public boolean waitForConfirmationOnAll(int timeout_ms) throws IOException {
        return false;
    }
    @Override public boolean close(int timeout_ms) throws IOException {
        return false;
    }
    @Override public boolean isClosed() {
        return false;
    }
}
