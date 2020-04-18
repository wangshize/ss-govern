package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.MasterNodeRole;
import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NodeInfo;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * controller候选人
 * @author wangsz
 * @create 2020-04-08
 **/
public class ControllerCandidate {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerCandidate.class);

    private MasterNetworkManager masterNetworkManager;

    public ControllerCandidate(MasterNetworkManager masterNetworkManager) {
        this.masterNetworkManager = masterNetworkManager;
    }

    /**
     * 投票选举controller
     * @return
     */
    public MasterNodeRole voteForControllerElection() {
        Integer selfNodeId = GovernServerConfig.getInstance().getNodeId();
        ConfigurationParser configParser = ConfigurationParser.getInstance();
        List<NodeInfo> allNodes = configParser.parseMasterNodeServers();
        for (NodeInfo node : allNodes) {
            if(node.getNodeId().equals(selfNodeId)) {
                continue;
            }
            ByteBuffer message = ByteBuffer.wrap((selfNodeId + "节点发出的投票").getBytes());
            masterNetworkManager.sendMessage(node.getNodeId(), message);
        }
        int count = 0;
        int otherNodeSize = allNodes.size() - 1;
        do {
            ByteBuffer recvMessage = masterNetworkManager.takeRecvMessage();
            if (LOG.isDebugEnabled()) {
                LOG.debug("接收到一个投票：" + new String(recvMessage.array()));
            }
            count++;
        } while (count < otherNodeSize);
        return MasterNodeRole.CANDIDATE;
    }
}
