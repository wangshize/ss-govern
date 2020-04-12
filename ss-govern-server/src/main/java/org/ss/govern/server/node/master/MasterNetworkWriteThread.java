package org.ss.govern.server.node.master;

import java.net.Socket;

/**
 * master节点间网络连接写线程
 * @author wangsz
 * @create 2020-04-11
 **/
public class MasterNetworkWriteThread extends Thread {

    /**
     * master节点间的网络连接
     */
    private Socket socket;

    public MasterNetworkWriteThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

    }
}
