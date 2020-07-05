package org.ss.govern.server.node;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.master.MasterConnectionListener;
import org.ss.govern.server.node.master.MasterNodePeer;
import org.ss.govern.server.node.master.NetworkReadThread;
import org.ss.govern.server.node.master.NetworkWriteThread;
import org.ss.govern.server.node.master.SlaveConnectionListener;
import org.ss.govern.server.node.slave.SlaveNodePeer;
import org.ss.govern.utils.ThreadUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class NetworkManager {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkManager.class);

    private final int DEFAULT_RETRIES = 3;
    private final int CONNECT_TIMEOUT = 5000;

    private NodeManager nodeManager;

    private GovernServerConfig config;

    /**
     * 等待重试发起连接的master列表
     */
    private List<NodeAddress> retryConnectOtherMasterNodes = new CopyOnWriteArrayList<>();

    /**
     * 其他远程节点建立好的连接
     * key nodeId
     * value socket
     */
    private ConcurrentHashMap<Integer, Socket> remoteNodeSockets = new ConcurrentHashMap<>();

    /**
     * master节点向外发送数据队列
     */
    private Map<Integer, LinkedBlockingQueue<ByteBuffer>> queueSendMap = new ConcurrentHashMap<>();

    /**
     * slave节点发送的数据接收队列
     */
    private Map<Integer, LinkedBlockingQueue<ByteBuffer>> slaveQueueRecvMap = new ConcurrentHashMap<>();

    /**
     * master节点发送的数据接收队列
     */
    private LinkedBlockingQueue<ByteBuffer> masterQueueRecv = new LinkedBlockingQueue<>();

    private NodeAddress self;

    public NetworkManager(NodeManager nodeManager) {
        this.config = GovernServerConfig.getInstance();
        this.nodeManager = nodeManager;
        ConfigurationParser configurationParser = ConfigurationParser.getInstance();
        this.self = configurationParser.getSelfNodePeer();
        new RetryConnectMasterNodeThread().start();
    }

    public void waitOtherMasterNodesConnect() {
        new MasterConnectionListener(this).start();
    }

    public void connectOtherMasterNodes() {
        List<NodeAddress> beforeMasterNodes = getBeforeMasterNodes();
        if (CollectionUtils.isEmpty(beforeMasterNodes)) {
            return;
        }
        for (NodeAddress beforeMasterNode : beforeMasterNodes) {
            connectBeforeMasterNode(beforeMasterNode);
        }
    }

    /**
     * 等待自己的slave节点发起连接
     */
    public void waitSlaveNodeConnect() {
        new SlaveConnectionListener(this).start();
    }

    /**
     * 等待大多数节点启动
     */
    public void waitMostNodesConnected() {
        //无需等待所有节点连接，只需要超过一半的节点建立成功即可开始选举
        Integer masterNumInCluster = nodeManager.getMasterNumInCluster();
        int mostNodeNum = masterNumInCluster / 2 + 1;
        while (NodeStatus.isRunning() && remoteNodeSockets.size() < mostNodeNum) {
            LOG.info("wait for other node connect....");
            ThreadUtils.sleep(2000);
        }
        LOG.info("most node connect successful.....`");
    }

    private boolean connectBeforeMasterNode(NodeAddress nodeInfo) {
        String ip = nodeInfo.getIp();
        int port = nodeInfo.getMasterConnectPort();
        int nodeId = nodeInfo.getNodeId();
        LOG.info("try to connect master node :" + ip + ":" + port);
        int retries = 0;
        while (NodeStatus.isRunning() && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(ip, port);
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(0);
                socket.connect(endpoint, CONNECT_TIMEOUT);
                LOG.info("successfully connected master node :" + ip + ":" + port);
                addSocket(nodeId, socket);
                addRemoteMasterNode(new MasterNodePeer(nodeId, true));
                if(!initiateConnection(socket, self.getNodeId())) {
                    break;
                }
                startMasterSocketIOThreads(nodeId, socket);
                return true;
            } catch (IOException e) {
                LOG.error("connect with " + ip + " fail");
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    LOG.info(String.format("connect with %s fail, retry to connect %s times",
                            ip, retries));
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

    /**
     * master节点响应其他master的连接
     * @param sock
     * @param sid
     */
    public boolean initiateConnection(final Socket sock, final Integer sid) {
        DataOutputStream dout;
        try {
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);

            GovernServerConfig serverConfig = GovernServerConfig.getInstance();
            dout.writeInt(sid);
            dout.writeInt(serverConfig.getIsControllerCandidate() ? 1 : 0);
            dout.flush();
            return true;
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }
    }

    /**
     * @param sock
     * @return remoteNodeId
     */
    public Integer receiveMasterConnection(final Socket sock) {
        DataInputStream din = null;
        Integer remoteNodeId = null;
        try {
            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
            remoteNodeId = din.readInt();
            boolean isControllerCandidate = din.readInt() == 1 ? true : false;
            nodeManager.updateNodeIsControllerCandidate(remoteNodeId, isControllerCandidate);
            addRemoteMasterNode(new MasterNodePeer(remoteNodeId, isControllerCandidate));
            addSocket(remoteNodeId, sock);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
        return remoteNodeId;
    }

    public Integer receiveSlaveConnection(final Socket sock) {
        DataInputStream din = null;
        Integer remoteNodeId = null;
        try {
            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
            remoteNodeId = din.readInt();
            addSocket(remoteNodeId, sock);
            addRemoteSlaveNode(new SlaveNodePeer(remoteNodeId));
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
        return remoteNodeId;
    }

    /**
     * 获取id比自己小的节点信息列表
     *
     * @return
     */
    private List<NodeAddress> getBeforeMasterNodes() {
        Integer nodeId = GovernServerConfig.getInstance().getNodeId();
        List<NodeAddress> beforeMasterNode = new ArrayList<>();
        ConfigurationParser configurationParser = ConfigurationParser.getInstance();
        List<NodeAddress> peers = configurationParser.parseMasterNodeServers();
        for (NodeAddress nodeInfo : peers) {
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
    public void addSocket(Integer nodeId, Socket client) {
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
            LOG.info("receive client's node id is " + nodeId + ",and put it in cache[" + remoteNodeSockets.keys() + "]");
            remoteNodeSockets.put(nodeId, client);
        }
    }

    /**
     * 添加一个远程master节点
     * @param masterNodePeer
     */
    public void addRemoteMasterNode(MasterNodePeer masterNodePeer) {
        nodeManager.addRemoteMasterNode(masterNodePeer);
    }

    /**
     * 添加一个远程slave节点
     * @param slaveNodePeer
     */
    public void addRemoteSlaveNode(SlaveNodePeer slaveNodePeer) {
        nodeManager.addRemoteSlaveNode(slaveNodePeer);
    }

    public Socket getConnectByNodeId(Integer nodeId) {
        return remoteNodeSockets.get(nodeId);
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

    public NodeAddress getSelf() {
        if (self != null) {
            return self;
        }
        Integer nodeId = GovernServerConfig.getInstance().getNodeId();
        LOG.error(String.format("nodeId = %s addr config can not find", nodeId));
        NodeStatus nodeStatus = NodeStatus.getInstance();
        nodeStatus.setStatus(NodeStatus.FATAL);
        return null;
    }

    public void startMasterSocketIOThreads(Integer remoteNodeId, Socket socket) {
        LinkedBlockingQueue<ByteBuffer> masterQueueSend = new LinkedBlockingQueue<>();
        if(queueSendMap.putIfAbsent(remoteNodeId, masterQueueSend) != null) {
            throw new IllegalArgumentException("nodeId : " + remoteNodeId + " is already exist");
        }
        new NetworkWriteThread(remoteNodeId, socket, masterQueueSend,this).start();
        new NetworkReadThread(remoteNodeId, socket, masterQueueRecv,this).start();
    }

    public void startSlvaeSocketIOThreads(Integer remoteNodeId, Socket socket) {
        LinkedBlockingQueue<ByteBuffer> slaveQueueSend = new LinkedBlockingQueue<>();
        if(queueSendMap.putIfAbsent(remoteNodeId, slaveQueueSend) != null) {
            throw new IllegalArgumentException("nodeId : " + remoteNodeId + " is already exist");
        }
        LinkedBlockingQueue<ByteBuffer> slaveQueueRecv = new LinkedBlockingQueue<>();
        if(slaveQueueRecvMap.putIfAbsent(remoteNodeId, slaveQueueRecv) != null) {
            throw new IllegalArgumentException("nodeId : " + remoteNodeId + " is already exist");
        }
        new NetworkWriteThread(remoteNodeId, socket, slaveQueueSend,this).start();
        new NetworkReadThread(remoteNodeId, socket, slaveQueueRecv,this).start();
    }

    /**
     * 向指定远程节点发送信息
     */
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

    /**
     * 阻塞式获取master消息
     *
     * @return
     */
    public ByteBuffer takeMasterRecvMessage() throws InterruptedException {
        return masterQueueRecv.take();
    }

    public void recvSlaveMessage(Integer nodeId, ByteBuffer message) {
        slaveQueueRecvMap.get(nodeId).offer(message);
    }

    class RetryConnectMasterNodeThread extends Thread {
        private final Logger LOG = LoggerFactory.getLogger(RetryConnectMasterNodeThread.class);
        private final int RETRY_CONNECT_MASTER_NODE_INTERVAL = 5 * 60 * 1000;

        @Override
        public void run() {
            while (NodeStatus.isRunning()) {
                List<NodeAddress> retryConnectSuccessNodes = new ArrayList<>();
                for (NodeAddress nodeInfo : retryConnectOtherMasterNodes) {
                    if (connectBeforeMasterNode(nodeInfo)) {
                        retryConnectSuccessNodes.add(nodeInfo);
                    }
                }
                for (NodeAddress successNode : retryConnectSuccessNodes) {
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
