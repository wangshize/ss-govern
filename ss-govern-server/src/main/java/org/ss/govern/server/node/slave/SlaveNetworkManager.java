package org.ss.govern.server.node.slave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.GovernServer;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeManager;
import org.ss.govern.server.node.NodeStatus;
import org.ss.govern.server.node.master.MasterNodePeer;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author wangsz
 * @create 2020-07-05
 **/
public class SlaveNetworkManager {

    private static final Logger LOG = LoggerFactory.getLogger(SlaveNetworkManager.class);

    private GovernServerConfig config;

    private Socket masterNodeSocket;

    private final int DEFAULT_RETRIES = 3;

    private final int CONNECT_TIMEOUT = 5000;

    /**
     * 数据接收队列
     */
    private LinkedBlockingQueue<ByteBuffer> queueRecv = new LinkedBlockingQueue<>();

    /**
     * 数据发送队列
     */
    private LinkedBlockingQueue<ByteBuffer> queueSend = new LinkedBlockingQueue<>();


    public SlaveNetworkManager() {
        this.config = GovernServerConfig.getInstance();
    }

    /**
     * 连接对应master节点
     */
    public void connectMasterNode() {
        String ip = config.getMasterNodeAddress();
        Integer port = config.getMasterNodePort();
        LOG.info("connecting master node:" + ip + ":" + port);
        int retries = 0;
        while (NodeStatus.isRunning() && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(ip, port);
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(0);
                socket.connect(endpoint, CONNECT_TIMEOUT);
                if(!initiateConnection(socket, config.getNodeId())) {
                    break;
                }
                startSocketIOThreads(socket);
                this.masterNodeSocket = socket;
                LOG.info("successfully connected master node :" + ip + ":" + port);
                return;
            } catch (IOException e) {
                String masterAddr = ip + ":" + port;
                LOG.error("connect with " + masterAddr + " fail");
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    LOG.info(String.format("connect with %s fail, retry to connect %s times",
                            masterAddr, retries));
                }
            }
        }
        NodeStatus nodeStatus = NodeStatus.getInstance();
        nodeStatus.setStatus(NodeStatus.FATAL);
        LOG.error("failed to listen other node's connection. going to shutdown system");
    }

    private void startSocketIOThreads(Socket socket) {
        new SlaveNetworkReadThread(socket, queueRecv, this).start();
        new SlaveNetworkWriteThread(socket,queueSend, this).start();
    }

    /**
     * 连接建立成功后，初始化操作
     *
     * @param sock
     * @param sid
     */
    public boolean initiateConnection(final Socket sock, final Integer sid) {
        DataOutputStream dout;
        try {
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);
            dout.writeInt(sid);
            dout.flush();
            return true;
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }
    }

    public void closeSocket(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

}
