package jokrey.utilities.network.link2peer.tcp_punching;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/** from: http://blog.boehme.me/2011/06/nat-holepunch-solution-in-java.html */
public class ClientBehindNAT {

    private String relayServerIP;
    private String destinationIP;
    private InetAddress clientIP;

    private int socketTimeout;

    public static void main(String[] args) {
        ClientBehindNAT c = new ClientBehindNAT();
        boolean result = c.parseInput(args);

        if(result){
            try {
                c.createHole(true);
            } catch (SocketTimeoutException e) {
                System.out.println("Timeout occured... Try again?");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else{
            System.err.println("Usage: <Relay Server IP> (e.g. 192.168.40.1) <Destination IP> (e.g. 192.168.40.128) <Timeout> (in milliseconds)");
        }

    }

    /**
     * Main method used for outer access
     * @par
     * @return the free NAT port of the Firewall
     * @throws IOException
     */
    public synchronized int createConnectionToPeer(String relayServerIP, String destinationIP, int socketTimeout) throws IOException{
        this.relayServerIP = relayServerIP;
        this.destinationIP = destinationIP;
        this.socketTimeout = socketTimeout;
        return createHole(false);
    }

    private boolean parseInput(String[] args){
        if(args.length < 3){
            return false;
        } else if(args[0].equals("") || args[1].equals("") || args[2].equals("")){
            return false;
        }else{
            relayServerIP = args[0];
            destinationIP = args[1];
            socketTimeout = Integer.valueOf(args[2]);
            return true;
        }
    }

    private int createHole(boolean standAlone) throws IOException{
//        InetAddress serverIPAddress = InetAddress.getByName("192.168.40.1");
        clientIP = InetAddress.getLocalHost();
        byte[] addrInByte = createInternetAddressFromString(relayServerIP);//{(byte) 192, (byte) 168, 40, 1};
        InetAddress serverIPAddress = InetAddress.getByAddress(addrInByte);
        int port = 12345;
        String data = destinationIP;
        System.out.println("Try to send: "+data+" TO: "+serverIPAddress+" ON PORT: "+port);
        //first send UDP packet to the relay server
        DatagramSocket clientSocket = new DatagramSocket();
        clientSocket.setSoTimeout(socketTimeout);
        sendPacket(clientSocket, serverIPAddress, port, data);
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
        int index = address.indexOf('.');
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
        byte[] sendData = new byte[data.length()];
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
                while(true){
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
                while(true){
                    sendPacket(clientSocket, destIPAddress, port, "Hello from "+clientIP);
                    Thread.sleep(2000);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
