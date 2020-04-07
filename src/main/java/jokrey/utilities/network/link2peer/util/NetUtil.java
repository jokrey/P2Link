package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.bitsandbytes.BitHelper;
import java.net.*;

/**
 * @author jokrey
 */
public class NetUtil {
    public static InterfaceAddress getLocalIPv4Interface() throws UnknownHostException, SocketException {
        InetAddress localHost = Inet4Address.getLocalHost();
        NetworkInterface networkInterface = NetworkInterface.getByInetAddress(localHost);
        return networkInterface.getInterfaceAddresses().stream().filter(it -> it.getAddress() instanceof Inet4Address).findFirst().orElse(null);
    }
    public static boolean isV4AndFromSameSubnet(InetAddress anyIP, InterfaceAddress localIPv4InterfaceAddress) {
        if(anyIP instanceof Inet4Address) {
            return isFromSameSubnet((Inet4Address) anyIP, localIPv4InterfaceAddress);
        } else {
            return false;
        }
    }
    public static boolean isFromSameSubnet(Inet4Address anyIPv4, InterfaceAddress localIPv4InterfaceAddress) {
        int ip = BitHelper.getInt32From(anyIPv4.getAddress());
        int localIp = BitHelper.getInt32From(localIPv4InterfaceAddress.getAddress().getAddress());
        int netmask = BitHelper.maskedInt32(localIPv4InterfaceAddress.getNetworkPrefixLength());
        int subnet = localIp & netmask;
//        System.out.println("ip = " + ip);
//        System.out.println("localIp = " + localIp);
//        System.out.println("netmask = " + netmask);
//        System.out.println("subnet = " + subnet);
//        System.out.println("ip.bytes = " + Arrays.toString(BitHelper.getBytes(ip)));
//        System.out.println("localIp.bytes = " + Arrays.toString(BitHelper.getBytes(localIp)));
//        System.out.println("netmask.bytes = " + Arrays.toString(BitHelper.getBytes(netmask)));
//        System.out.println("subnet.bytes = " + Arrays.toString(BitHelper.getBytes(subnet)));
        return (ip & netmask) == (subnet & netmask);
    }
}
