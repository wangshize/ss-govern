package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.MasterNodeRole;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.MessageReceiver;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeManager;
import org.ss.govern.server.node.SlotsManager;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class MasterNode {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNode.class);

    private NetworkManager networkManager;

    private NodeManager nodeManager;

    private MessageReceiver messageReceiver;

    private SlotsManager slotsManager;

    private GovernServerConfig serverConfig;

    public MasterNode() {
        this.nodeManager = new NodeManager();
        this.networkManager = new NetworkManager(nodeManager);
        this.serverConfig = GovernServerConfig.getInstance();
        this.messageReceiver = new MessageReceiver(networkManager);
        this.slotsManager = new SlotsManager(messageReceiver);
    }

    public void start() throws InterruptedException {
        //启动消息接收器
        messageReceiver.start();
        //等待id大于自己的节点来连接
        networkManager.waitOtherMasterNodesConnect();
        //连接id小于自己的master节点
        networkManager.connectOtherMasterNodes();
        //等待所有数节点启动
        networkManager.waitAllNodesConnected();
        //选举controller
        Boolean isController = false;
        Boolean isControllerCandidate = serverConfig.getIsControllerCandidate();
        if (isControllerCandidate) {
            ControllerCandidate controllerCandidate = new ControllerCandidate(networkManager, nodeManager, messageReceiver);
            MasterNodeRole role = controllerCandidate.voteForControllerElection();
            LOG.info("vote finish, Current NodeRole is " + role);
            if (MasterNodeRole.CONTROLLER.equals(role)) {
                Controller controller = new Controller(nodeManager, networkManager);
                controller.allocateSlots();
                isController = true;
            }
        }
        //等待接收分配的槽位数据
        if(!isController) {
            slotsManager.waitForSlotsAllocation();
        }
        //启动线程监听slave节点发起的连接请求
        networkManager.waitSlaveNodeConnect();
    }

}
