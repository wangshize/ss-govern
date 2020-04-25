package org.ss.govern.server.node;

import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点管理组件
 * @author wangsz
 * @create 2020-04-08
 **/
public class RemoteNodeManager {

    private Map<Integer, NodeInfo> remoteMasterNodes = new ConcurrentHashMap<>();

    private GovernServerConfig serverConfig;

    public Map<Integer, NodeInfo> getRemoteMasterNodes() {
        return remoteMasterNodes;
    }

    public void updateNodeIsControllerCandidate(int nodeId, boolean isControllerCandidate) {
        NodeInfo nodeInfo = remoteMasterNodes.get(nodeId);
        if(nodeInfo != null) {
            nodeInfo.setControllerCandidate(isControllerCandidate);
        }
    }

//    public void addRemoteNode(Integer remoteNodeId, NodeInfo node) {
//        remoteNodes.put(remoteNodeId, node);
//    }

    public List<NodeInfo> getOtherControllerCandidate() {
        Integer selfNodeId = serverConfig.getNodeId();
        List<NodeInfo> allOtherControllerCandidates = new ArrayList<>(remoteMasterNodes.size());
        for (NodeInfo value : remoteMasterNodes.values()) {
            if(!value.getNodeId().equals(selfNodeId)
            && value.getControllerCandidate()) {
                allOtherControllerCandidates.add(value);
            }
        }
        return allOtherControllerCandidates;
    }

    public RemoteNodeManager() {
        this.serverConfig = GovernServerConfig.getInstance();
        ConfigurationParser configurationParser = ConfigurationParser.getInstance();
        List<NodeInfo> nodeInfoList = configurationParser.parseMasterNodeServers();
        for (NodeInfo nodeInfo : nodeInfoList) {
            remoteMasterNodes.put(nodeInfo.getNodeId(), nodeInfo);
        }
    }

}
