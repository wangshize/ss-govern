package org.ss.govern.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class NetUtils {

    public static String formatInetAddr(InetSocketAddress addr) {
        InetAddress ia = addr.getAddress();

        if (ia == null) {
            return String.format("%s:%s", addr.getHostString(), addr.getPort());
        }

        if (ia instanceof Inet6Address) {
            return String.format("[%s]:%s", ia.getHostAddress(), addr.getPort());
        } else {
            return String.format("%s:%s", ia.getHostAddress(), addr.getPort());
        }
    }
}