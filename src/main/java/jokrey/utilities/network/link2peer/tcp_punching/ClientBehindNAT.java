package jokrey.utilities.network.link2peer.tcp_punching;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;

/** from: http://blog.boehme.me/2011/06/nat-holepunch-solution-in-java.html */
public class ClientBehindNAT {
    public static int RELAY_SERVER_PORT = 43288;
    public static String RELAY_SERVER_IP = "localhost";
//    public static String RELAY_SERVER_IP = "lmservicesip.ddns.net";

    private String destinationIP = "test1";
    private InetAddress clientIP;

    private int socketTimeout = 3000;

    public static void main(String[] args) {
        ClientBehindNAT c = new ClientBehindNAT();

        try {
            c.createHole(true);
        } catch (SocketTimeoutException e) {
            System.out.println("Timeout occured... Try again?");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main method used for outer access
     * @return the free NAT port of the Firewall
     */
    public synchronized int createConnectionToPeer(String relayServerIP, String destinationIP, int socketTimeout) throws IOException{
        this.destinationIP = destinationIP;
        this.socketTimeout = socketTimeout;
        return createHole(false);
    }

    private int createHole(boolean standAlone) throws IOException{
//        InetAddress serverIPAddress = InetAddress.getByName("192.168.40.1");
        clientIP = InetAddress.getLocalHost();
//        byte[] addrInByte = createInternetAddressFromString(relayServerIP);//{(byte) 192, (byte) 168, 40, 1};
        InetAddress serverIPAddress = InetAddress.getByName(RELAY_SERVER_IP);
        String data = destinationIP;
        System.out.println("Try to send: "+data+" TO: "+serverIPAddress+" ON PORT: "+RELAY_SERVER_PORT);
        //first send UDP packet to the relay server
        DatagramSocket clientSocket = new DatagramSocket();
        clientSocket.setSoTimeout(socketTimeout);
        sendPacket(clientSocket, serverIPAddress, RELAY_SERVER_PORT, data);
        //now wait for the answer of the server
        String peer = receivePacket(clientSocket);
        clientSocket.close();
        //need to make sure all had the chance to receive packets with infos
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return initPeerConnection(peer, standAlone);
    }

    private byte[] createInternetAddressFromString(String address){
        byte[] inetAddressBytes = new byte[4];
        int index;
        int i=0;
        while((index = address.indexOf('.')) != -1){
            int part = Integer.valueOf(address.substring(0, index));
            inetAddressBytes[i] = (byte) part;
            address = address.substring(index+1);
            i++;
        }
        inetAddressBytes[i] = (byte) Integer.valueOf(address).intValue();
        return inetAddressBytes;
    }

    private synchronized void sendPacket(DatagramSocket socket, InetAddress destIPAddress, int port, String data) throws IOException{
        byte[] sendData;
        sendData = data.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData,
                sendData.length, destIPAddress, port);
        socket.send(sendPacket);
    }

    private synchronized String receivePacket(DatagramSocket socket) throws IOException{
        byte[] receiveData = new byte[50];
        DatagramPacket receivePacket = new DatagramPacket(receiveData,
                receiveData.length);
        socket.receive(receivePacket);

        String answerFromServer = new String(receivePacket.getData());
        answerFromServer = answerFromServer.substring(0, answerFromServer.indexOf(0));
        System.out.println(clientIP+" GOT FROM SERVER:" + answerFromServer);
        return answerFromServer;
    }

    private synchronized int initPeerConnection(String peer, boolean standAlone) throws IOException{
        String accordingIP = peer.substring(0, peer.indexOf(':'));
        String accordingPort = peer.substring(peer.indexOf(':')+1, peer.indexOf('-'));
        InetAddress destIPAddress = InetAddress.getByName(accordingIP);
        int remotePort = Integer.valueOf(accordingPort);
        int natPort = Integer.valueOf(peer.substring(peer.indexOf('-')+1));
        //this only applies in stand-alone mode
        if(standAlone){
            Thread receiver = new Thread(new PeerReceiveThread(natPort));
            receiver.start();
            Thread sender = new Thread(new PeerSendThread(destIPAddress, remotePort));
            sender.start();
        }

        return natPort;
    }

    private class PeerReceiveThread implements Runnable{

        int natPort;

        public PeerReceiveThread(int port){
            this.natPort = port;
        }

        public void run() {
            try {
                DatagramSocket serverSocket = new DatagramSocket(natPort);
                byte[] receiveData = new byte[50];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                System.out.println(clientIP+" listening on port: "+natPort);
                while(natPort!=-1){
                    serverSocket.receive(receivePacket);
                    String data = new String(receivePacket.getData());
                    System.out.println(clientIP+" FROM "+destinationIP+" WITH "+data);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class PeerSendThread implements Runnable{

        InetAddress destIPAddress;
        int port;

        public PeerSendThread(InetAddress destIPAddress, int port) {
            this.destIPAddress = destIPAddress;
            this.port = port;
        }

        public void run() {
            try {
                DatagramSocket clientSocket = new DatagramSocket();
                while(destinationIP!=null){
                    sendPacket(clientSocket, destIPAddress, port, "Hello from "+clientIP);
                    Thread.sleep(2000);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
