package org.ss.govern.server.node.master;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NodeInfo;
import org.ss.govern.server.node.NodeStatus;
import org.ss.govern.utils.NetUtils;
import org.ss.govern.utils.ThreadUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 集群节点间的通信管理组件
 * master和master  master和slave之间通信
 * 1、和其他master节点建立网络连接，避免出现重复的链接
 * 2、底层基于队列和线程，发送请求给其他节点，接收其他节点
 * 发送过来的请求放入接收队列
 *
 * @author wangsz
 * @create 2020-04-09
 **/
public class MasterNetworkManager {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNetworkManager.class);

    private final int DEFAULT_RETRIES = 3;
    private final int CONNECT_TIMEOUT = 5000;

    public static final long PROTOCOL_VERSION = -65536L;

    /**
     * 等待重试发起连接的master列表
     */
    private List<NodeInfo> retryConnectOtherMasterNodes = new CopyOnWriteArrayList<>();

    /**
     * 其他远程master节点建立好的连接
     * key nodeId
     * value socket
     */
    private ConcurrentHashMap<Integer, Socket> remoteNodeSockets = new ConcurrentHashMap<>();

    private Map<Integer, LinkedBlockingQueue<ByteBuffer>> queueSendMap = new ConcurrentHashMap<>();
    private LinkedBlockingQueue<ByteBuffer> queueRecv = new LinkedBlockingQueue<>();

    List<NodeInfo> nodeInfoList;

    private NodeInfo self;

    public MasterNetworkManager() {
        ConfigurationParser configurationParser = ConfigurationParser.getInstance();
        this.nodeInfoList = configurationParser.parseMasterNodeServers();
        this.self = getSelf();
        new RetryConnectMasterNodeThread().start();
    }

    public void waitOtherMasterNodesConnect() {
        new MasterConnectionListener().start();
    }

    public void connectOtherMasterNodes() {
        List<NodeInfo> beforeMasterNodes = getBeforeMasterNodes();
        if (CollectionUtils.isEmpty(beforeMasterNodes)) {
            return;
        }
        for (NodeInfo beforeMasterNode : beforeMasterNodes) {
            connectBeforeMasterNode(beforeMasterNode);
        }
    }

    /**
     * 等待大多数节点启动
     */
    public void waitMostNodesConnected() {
        int allOtherNodeNum = nodeInfoList.size() - 1;
        int mostNodeNum = allOtherNodeNum / 2 + 1;
        while (NodeStatus.isRunning() && remoteNodeSockets.size() < mostNodeNum) {
            LOG.info("wait for other node connect....");
            ThreadUtils.sleep(2000);
        }
        LOG.info("all node connect successful.....`");
    }

    private boolean connectBeforeMasterNode(NodeInfo nodeInfo) {
        int retries = 0;
        String ip = nodeInfo.getIp();
        int port = nodeInfo.getMasterConnectPort();
        int nodeId = nodeInfo.getNodeId();
        LOG.info("try to connect master node :" + ip + ":" + port);
        while (NodeStatus.isRunning() && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(ip, port);
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(0);
                socket.connect(endpoint, CONNECT_TIMEOUT);
                LOG.info("successfully connected master node :" + ip + ":" + port);
                initiateConnection(socket, self.getNodeId());
                addSocket(nodeId, socket);
                startIOThreads(nodeId, socket);
                return true;
            } catch (IOException e) {
                LOG.error("connect with " + nodeInfo.getIp() + " fail");
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    LOG.info(String.format("connect with %s fail, retry to connect %s times",
                            nodeInfo.getIp(), retries));
                }
            }
        }
        if (!retryConnectOtherMasterNodes.contains(nodeInfo)) {
            retryConnectOtherMasterNodes.add(nodeInfo);
            LOG.error("retried connect master node :" + ip + ":" + port + "failed, " +
                    "and it into retry connect list");
        }
        return false;
    }

    public void initiateConnection(final Socket sock, final Integer sid) {
        DataOutputStream dout = null;
        DataInputStream din = null;
        try {
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);

            // Sending id and challenge
            // represents protocol version (in other words - message type)
            dout.writeLong(PROTOCOL_VERSION);
            dout.writeInt(sid);
            dout.flush();

            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
        }
    }

    /**
     * @param sock
     * @return remoteNodeId
     */
    public Integer receiveConnection(final Socket sock) {
        DataInputStream din = null;
        Integer sid = null;
        try {
            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
            //预留
            Long protocolVersion = din.readLong();
            sid = din.readInt();
            addSocket(sid, sock);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
        return sid;
    }

    /**
     * 获取id比自己小的节点信息列表
     *
     * @return
     */
    private List<NodeInfo> getBeforeMasterNodes() {
        ConfigurationParser parser = ConfigurationParser.getInstance();
        nodeInfoList = parser.parseMasterNodeServers();
        Integer nodeId = GovernServerConfig.getInstance().getNodeId();
        List<NodeInfo> beforeMasterNode = new ArrayList<>();
        for (NodeInfo nodeInfo : nodeInfoList) {
            if (nodeInfo.getNodeId() < nodeId) {
                beforeMasterNode.add(nodeInfo);
            }
        }
        return beforeMasterNode;
    }

    /**
     * 缓存建立好的连接
     *
     * @param client
     */
    private void addSocket(Integer nodeId, Socket client) {
        InetSocketAddress remoteAddr = (InetSocketAddress) client.getRemoteSocketAddress();
        String remoteAddrHostName = remoteAddr.getHostName();
        if (nodeId == null) {
            //接收到了没有再配置文件里的其他节点的连接
            LOG.error("established connection is not in the right remote address " + remoteAddrHostName + " nodeId = " + nodeId);
            try {
                client.close();
            } catch (IOException e) {
                LOG.error("close connection in unknown remote address failed", e);
            }
        } else {
            LOG.info("receive client's node id is " + nodeId + ",and put it in cache");
            remoteNodeSockets.putIfAbsent(nodeId, client);
        }
    }

    public void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }
        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    private NodeInfo getSelf() {
        Integer nodeId = GovernServerConfig.getInstance().getNodeId();
        for (NodeInfo nodeInfo : nodeInfoList) {
            if (nodeInfo.getNodeId().equals(nodeId)) {
                return nodeInfo;
            }
        }
        LOG.error(String.format("nodeId = %s addr config can not find", nodeId));
        NodeStatus nodeStatus = NodeStatus.getInstance();
        nodeStatus.setStatus(NodeStatus.FATAL);
        return null;
    }

    private void startIOThreads(Integer remoteNodeId, Socket socket) {
        LinkedBlockingQueue<ByteBuffer> sendQueue = new LinkedBlockingQueue<>();
        queueSendMap.put(remoteNodeId, sendQueue);
        new MasterNetworkWriteThread(remoteNodeId, socket, this).start();
        new MasterNetworkReadThread(remoteNodeId, socket, this).start();
    }

    public Boolean sendMessage(Integer remoteNodeId, ByteBuffer request) {
        try {
            LinkedBlockingQueue<ByteBuffer> sendQueue = queueSendMap.get(remoteNodeId);
            sendQueue.put(request);
        } catch (InterruptedException e) {
            LOG.error("put request into sendQueue error, remoteNodeId = " + remoteNodeId, e);
            return false;
        }
        return true;
    }

    public void removeSendQueue(Integer nodeId) {
        this.queueSendMap.remove(nodeId);
    }

    public ByteBuffer takeSendQueue(Integer nodeId) throws InterruptedException {
        LinkedBlockingQueue<ByteBuffer> queue = queueSendMap.get(nodeId);
        if(queue == null) {
            throw new IllegalArgumentException("error nodeId, can not find sendQueue by this nodeId:" + nodeId);
        }
        return queue.take();
    }

    /**
     * 阻塞式获取消息
     * @return
     */
    public ByteBuffer takeRecvMessage() {
        try {
            return queueRecv.take();
        } catch (InterruptedException e) {
            LOG.error("take message from queueRecv error", e);
            return null;
        }
    }

    public void addToRecvQueue(ByteBuffer message) throws InterruptedException {
        queueRecv.put(message);
    }

    /**
     * 网络连接监听器
     *
     * @author wangsz
     * @create 2020-04-10
     **/
    class MasterConnectionListener extends Thread {

        private final Logger LOG = LoggerFactory.getLogger(MasterConnectionListener.class);

        private final int DEFAULT_RETRIES = 3;

        private ServerSocket serverSocket;

        /**
         * 已重试次数
         */
        private int retyies = 0;

        public MasterConnectionListener() {
            init();
        }

        private void init() {
            GovernServerConfig config = GovernServerConfig.getInstance();
        }


        @Override
        public void run() {
            Socket client = null;
            while (NodeStatus.isRunning() && retyies <= DEFAULT_RETRIES) {
                try {
                    int port = self.getMasterConnectPort();
                    InetSocketAddress endpoint = new InetSocketAddress(port);
                    serverSocket = new ServerSocket();
                    //与其他节点意外断开连接后，此时连接处于timeout状态，无法重新绑定端口号
                    //设置为true后，允许重新对端口号进行绑定连接
                    //也就说服务端此时还没有真正关闭这个端口
                    serverSocket.setReuseAddress(true);
                    serverSocket.bind(endpoint);
                    LOG.info("binding port " + port + " success");
                    while (NodeStatus.isRunning()) {
                        client = this.serverSocket.accept();
                        setSockOpts(client);
                        LOG.info("Received connection request "
                                + NetUtils.formatInetAddr((InetSocketAddress) client.getRemoteSocketAddress()));
                        Integer remoteNodeId = receiveConnection(client);
                        if(remoteNodeId != null) {
                            startIOThreads(remoteNodeId, client);
                        }
                        retyies = 0;
                    }
                } catch (IOException e) {
                    if (!NodeStatus.isRunning()) {
                        break;
                    }
                    LOG.error("Exception while listening", e);
                    retyies++;
                    if (retyies <= DEFAULT_RETRIES) {
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

        private void setSockOpts(Socket sock) throws SocketException {
            sock.setTcpNoDelay(true);
            //读取数据时超时时间为0，即没有超时时间，阻塞读取
            sock.setSoTimeout(0);
        }

    }

    class RetryConnectMasterNodeThread extends Thread {
        private final Logger LOG = LoggerFactory.getLogger(RetryConnectMasterNodeThread.class);
        private final int RETRY_CONNECT_MASTER_NODE_INTERVAL = 5 * 60 * 1000;

        @Override
        public void run() {
            while (NodeStatus.isRunning()) {
                List<NodeInfo> retryConnectSuccessNodes = new ArrayList<>();
                for (NodeInfo nodeInfo : retryConnectOtherMasterNodes) {
                    if (connectBeforeMasterNode(nodeInfo)) {
                        retryConnectSuccessNodes.add(nodeInfo);
                    }
                }
                for (NodeInfo successNode : retryConnectSuccessNodes) {
                    retryConnectOtherMasterNodes.remove(successNode);
                }
                try {
                    Thread.sleep(RETRY_CONNECT_MASTER_NODE_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("retryConnectMasterNodeThread Interrupted while sleeping. " +
                            "Ignoring exception", e);
                }
            }
        }
    }

}
