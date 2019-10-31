package jokrey.utilities.network.link2peer.tcp_punching;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/** from: http://blog.boehme.me/2011/06/nat-holepunch-solution-in-java.html */
public class RelayServer {
    private Map<String, NATDevice> inquiredComputers = new HashMap<>();

    public static void main(String[] args) {
        RelayServer rs = new RelayServer();
//        try {
            rs.startServer();
//        } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//        }
    }

//    public static void

    private void startServer() {
        //general idea:
        //    get request for connection to light(non-public) peer (light peers have special links and can only be accessed via nodes that are already connected to them)
        //    such request can only be received by public peers
        // !if requester is public:
        //    send light peer the requester link - if the light peer chooses, it can establish a connection to the requester
        // !if requester is light (-> nat hole punching):
        //    have requester establish a second connection to this, public, node (on the same port, because not every port is necessarily free)
        //    that received second connection's details are then forwarded to the requester


//        DatagramSocket serverSocket = new DatagramSocket(RELAY_SERVER_PORT);
//        byte[] receiveDataBuffer = new byte[50];
//
//        while (true) {
//            DatagramPacket receivePacket = new DatagramPacket(receiveDataBuffer, receiveDataBuffer.payloadLength);
//            System.out.println("Listening...");
//            serverSocket.receive(receivePacket);
//            String idOfRequestedPeer = new String(receivePacket.getData());
//
//            //NAT Device 1
//            InetAddress incomingIPAddress = receivePacket.getAddress();
//            int port = receivePacket.getPort();
//            NATDevice startComp = new NATDevice();
//            startComp.setPortOfNAT(port);
//            startComp.setPublicIpOfNAT(incomingIPAddress);
//
//            inquiredComputers.put(idOfRequestedPeer)
//
//            System.out.println("Checking: " + startComp.getPublicIpOfNAT() + ":" + startComp.getPortOfNAT() + " TO: " + destinationComp.getPublicIpOfNAT());
//            //check here if a matching entry is already present
//            NATDevice matchComp = checkMatchingNATDevice(destinationComp);
//            if (matchComp != null) {
//                System.out.println("Already present.. now sending packets to both of them...");
//                //send to the socket from previously saved first NAT Device infos from the second NAT Device
//                send(serverSocket, matchComp, startComp);
//                Thread.sleep(1000);
//                send(serverSocket, startComp, matchComp);
//
//                inquiredComputers.remove(startComp);
//                inquiredComputers.remove(matchComp);
//            } else {
//                System.out.println("Adding " + startComp.getPublicIpOfNAT() + ":" + startComp.getPortOfNAT() + " AND " + destinationComp.getPublicIpOfNAT() + " for later matching");
//                inquiredComputers.add(startComp);
//            }
//        }

//        serverSocket.close();
    }

    private synchronized void sendPacket(DatagramSocket socket, NATDevice natDeviceHome, NATDevice natDeviceRemote) throws IOException{
        byte[] sendData = new byte[50];
        InetAddress homeIPAddress = natDeviceHome.getPublicIpOfNAT();
        int homePort =  natDeviceHome.getPortOfNAT();

        //now the remote destination
        InetAddress destIPAddress = natDeviceRemote.getPublicIpOfNAT();
        int destPort =  natDeviceRemote.getPortOfNAT();
        String data = destIPAddress.getHostAddress()+":"+destPort+"-"+homePort;
        sendData = data.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, homeIPAddress, homePort);
        socket.send(sendPacket);
    }














    class NATDevice {
        private int portOfNat;
        private InetAddress publicIpOfNAT;
        public void setPortOfNAT(int port) { this.portOfNat = port; }
        public void setPublicIpOfNAT(InetAddress publicIpOfNAT) { this.publicIpOfNAT = publicIpOfNAT; }
        public InetAddress getPublicIpOfNAT() { return publicIpOfNAT; }
        public int getPortOfNAT() { return portOfNat; }

        @Override
        public String toString() {
            return "NATDevice{" +
                    "portOfNat=" + portOfNat +
                    ", publicIpOfNAT=" + publicIpOfNAT +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NATDevice natDevice = (NATDevice) o;
            return portOfNat == natDevice.portOfNat &&
                    Objects.equals(publicIpOfNAT, natDevice.publicIpOfNAT);
        }

        @Override
        public int hashCode() {
            return Objects.hash(portOfNat, publicIpOfNAT);
        }
    }
}
