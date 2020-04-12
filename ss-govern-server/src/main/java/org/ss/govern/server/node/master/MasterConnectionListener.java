package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NodeInfo;
import org.ss.govern.server.node.NodeStatus;
import org.ss.govern.utils.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

/**
 * 网络连接监听器
 * @author wangsz
 * @create 2020-04-10
 **/
public class MasterConnectionListener extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(MasterConnectionListener.class);

    private static final int DEFAULT_RETRIES = 3;

    private ServerSocket serverSocket;

    private volatile  boolean running = true;

    private List<NodeInfo> nodeInfoList;

    private Integer nodeId;

    /**
     * 已重试次数
     */
    private int retyies = 0;

    public MasterConnectionListener() {
        init();
    }

    private void init() {
        GovernServerConfig config = GovernServerConfig.getInstance();
        ConfigurationParser parser = ConfigurationParser.getInstance();
        this.nodeInfoList = parser.parseMasterNodeServers();
        this.nodeId = config.getNodeId();
    }



    @Override
    public void run() {
        Socket client = null;
        while (NodeStatus.isRunning() && retyies <= DEFAULT_RETRIES) {
            try {
                NodeInfo self = getSelf(nodeInfoList);
                if(self == null) {
                    LOG.error(String.format("nodeId = %s addr config can not find", nodeId));
                    return;
                }
                int port = self.getMasterConnectPort();
                InetSocketAddress endpoint = new InetSocketAddress(port);
                serverSocket = new ServerSocket();
                //与其他节点意外断开连接后，此时连接处于timeout状态，无法重新绑定端口号
                //设置为true后，允许重新对端口号进行绑定连接
                serverSocket.setReuseAddress(true);
                serverSocket.bind(endpoint);
                LOG.info("binding port " + port + " success");
                while (NodeStatus.isRunning()) {
                    client = this.serverSocket.accept();
                    setSockOpts(client);
                    LOG.info("Received connection request "
                            + NetUtils.formatInetAddr((InetSocketAddress)client.getRemoteSocketAddress()));
                    startIOThreads(client);
                    retyies = 0;
                }
            } catch (IOException e) {
                if(!running) {
                    break;
                }
                LOG.error("Exception while listening", e);
                retyies++;
                if(retyies <= DEFAULT_RETRIES) {
                    LOG.info(retyies + " times retry to listen other master node connect");
                }
                try {
                    serverSocket.close();
                    Thread.sleep(1000);
                } catch (IOException ie) {
                    LOG.error("Error closing server socket", ie);
                } catch (InterruptedException ie) {
                    LOG.error("MasterConnectionListener Interrupted while sleeping. " +
                            "Ignoring exception", ie);
                }
                closeSocket(client);
            }
        }
        NodeStatus nodeStatus = NodeStatus.getInstance();
        nodeStatus.setStatus(NodeStatus.FATAL);
        LOG.error("failed to listen other master node's connection. going to shutdown system");
    }

    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    private NodeInfo getSelf(List<NodeInfo> nodeInfoList) {
        for (NodeInfo nodeInfo : nodeInfoList) {
            if(nodeInfo.getNodeId().equals(this.nodeId)) {
                return nodeInfo;
            }
        }
        return null;
    }

    private void startIOThreads(Socket socket) {
        new MasterNetworkWriteThread(socket).start();
        new MasterNetworkReadThread(socket).start();
    }

    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        //读取数据时超时时间为0，即没有超时时间，阻塞读取
        sock.setSoTimeout(0);
    }

}
