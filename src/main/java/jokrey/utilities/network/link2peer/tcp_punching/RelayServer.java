package jokrey.utilities.network.link2peer.tcp_punching;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;


public class RelayServer {

    private Set<NATDevice> inquiredComputers = new HashSet<NATDevice>();

    class NATDevice {
        private int portOfNat;
        private InetAddress publicIpOfNAT;
        public void setPortOfNAT(int port) { this.portOfNat = port; }
        public void setPublicIpOfNAT(InetAddress publicIpOfNAT) { this.publicIpOfNAT = publicIpOfNAT; }
        public InetAddress getPublicIpOfNAT() { return publicIpOfNAT; }
        public int getPortOfNAT() { return portOfNat; }
    }

    public static void main(String[] args) {
        RelayServer rs = new RelayServer();
        try {
            rs.startServer();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void startServer() throws IOException, InterruptedException {
        DatagramSocket serverSocket = new DatagramSocket(12345);
        byte[] receiveData = new byte[50];

        while (true) {
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            System.out.println("Listening...");
            serverSocket.receive(receivePacket);
            String data = new String(receivePacket.getData());

            //NAT Device 1
            InetAddress incomingIPAddress = receivePacket.getAddress();
            int port = receivePacket.getPort();
            NATDevice startComp = new NATDevice();
            startComp.setPortOfNAT(port);
            startComp.setPublicIpOfNAT(incomingIPAddress);

            //NAT Device 2
            String accordingIP = data.substring(0, data.indexOf(0));
            NATDevice destinationComp = new NATDevice();
            destinationComp.setPublicIpOfNAT(InetAddress.getByName(accordingIP));

            System.out.println("Checking: "+startComp.getPublicIpOfNAT()+":"+startComp.getPortOfNAT()+" TO: "+destinationComp.getPublicIpOfNAT());
            //check here if a matching entry is already present
            NATDevice matchComp = checkMatchingNATDevice(destinationComp);
            if(matchComp != null){
                System.out.println("Already present.. now sending packets to both of them...");
                //send to the socket from previously saved first NAT Device infos from the second NAT Device
                sendPacket(serverSocket, matchComp, startComp);
                Thread.sleep(1000);
                sendPacket(serverSocket, startComp, matchComp);

                inquiredComputers.remove(startComp);
                inquiredComputers.remove(matchComp);
            } else{
                System.out.println("Adding "+startComp.getPublicIpOfNAT()+":"+startComp.getPortOfNAT()+" AND "+destinationComp.getPublicIpOfNAT()+" for later matching");
                inquiredComputers.add(startComp);
            }
        }

//        serverSocket.close();
    }

    private synchronized NATDevice checkMatchingNATDevice(NATDevice comp){
        for(NATDevice c : inquiredComputers){
            if(c.getPublicIpOfNAT().equals(comp.getPublicIpOfNAT())){
                return c;
            }
        }
        return null;
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

}
