package org.ss.govern.server.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.utils.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

/**
 * @author wangsz
 * @create 2020-05-17
 **/
public abstract class AbstractConnectionListener extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectionListener.class);

    protected ServerSocket serverSocket;

    /**
     * 已重试次数
     */
    protected int retyies = 0;

    private final int DEFAULT_RETRIES = 3;

    /**
     * 绑定端口
     */
    protected int bindPort;

    private NetworkManager networkManager;

    public AbstractConnectionListener(NetworkManager networkManager) {
        this.networkManager = networkManager;
    }

    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        //读取数据时超时时间为0，即没有超时时间，阻塞读取
        sock.setSoTimeout(0);
    }

    @Override
    public void run() {
        Socket client = null;
        while (NodeStatus.isRunning() && retyies <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(bindPort);
                serverSocket = new ServerSocket();
                //与其他节点意外断开连接后，此时连接处于timeout状态，无法重新绑定端口号
                //也就说服务端此时还没有真正关闭这个端口
                //设置为true后，允许重新对端口号进行绑定连接
                serverSocket.setReuseAddress(true);
                serverSocket.bind(endpoint);
                LOG.info("binding port " + bindPort + " success");
                while (NodeStatus.isRunning()) {
                    client = this.serverSocket.accept();
                    setSockOpts(client);
                    LOG.info("Received connection request "
                            + NetUtils.formatInetAddr((InetSocketAddress) client.getRemoteSocketAddress()));
                    doAccept(client);
                    retyies = 0;
                }
            } catch (IOException e) {
                if (!NodeStatus.isRunning()) {
                    break;
                }
                LOG.error("Exception while listening", e);
                retyies++;
                if (retyies <= DEFAULT_RETRIES) {
                    LOG.info(retyies + " times retry to listen other node connect");
                }
                closeSocket(client);
            }
        }
        NodeStatus nodeStatus = NodeStatus.getInstance();
        nodeStatus.setStatus(NodeStatus.FATAL);
        LOG.error("failed to listen other node's connection. going to shutdown system");
    }

    protected boolean checkNodeIsConnected(Integer nodeId) {
        return networkManager.getConnectByNodeId(nodeId) != null;
    }

    private void closeSocket(Socket client) {
        try {
            serverSocket.close();
            Thread.sleep(1000);
        } catch (IOException ie) {
            LOG.error("Error closing server socket", ie);
        } catch (InterruptedException ie) {
            LOG.error("ConnectionListener Interrupted while sleeping. " +
                    "Ignoring exception", ie);
        }
        networkManager.closeSocket(client);
    }

    protected abstract void doAccept(Socket client);
}
