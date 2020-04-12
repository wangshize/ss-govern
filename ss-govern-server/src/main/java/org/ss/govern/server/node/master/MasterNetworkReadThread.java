package org.ss.govern.server.node.master;

import java.net.Socket;

/**
 * master节点间网络连接读线程
 * @author wangsz
 * @create 2020-04-11
 **/
public class MasterNetworkReadThread extends Thread {

    private Socket socket;

    public MasterNetworkReadThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

    }
}
