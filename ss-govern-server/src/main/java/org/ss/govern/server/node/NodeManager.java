package org.ss.govern.server.node;

import lombok.Getter;
import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.master.MasterNodePeer;
import org.ss.govern.server.node.slave.SlaveNodePeer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点管理组件
 * @author wangsz
 * @create 2020-04-08
 **/
public class NodeManager {

    /**
     * 远程master节点集合
     */
    private Map<Integer, MasterNodePeer> masterNodePeerMap = new ConcurrentHashMap<>();

    /**
     * 远程slave节点集合
     */
    private Map<Integer, SlaveNodePeer> slaveNodePeerMap = new ConcurrentHashMap<>();

    private GovernServerConfig serverConfig;

    /**
     * master的数量 包含自己
     */
    @Getter
    private Integer masterNumInCluster;

    public void updateNodeIsControllerCandidate(int nodeId, boolean isControllerCandidate) {
        MasterNodePeer nodeInfo = masterNodePeerMap.get(nodeId);
        if(nodeInfo != null) {
            nodeInfo.setIsControllerCandidate(isControllerCandidate);
        }
    }

    /**
     * 添加一个远程master节点
     * @param masterNodePeer
     */
    public void addRemoteMasterNode(MasterNodePeer masterNodePeer) {
        masterNodePeerMap.put(masterNodePeer.getNodeId(), masterNodePeer);
    }

    /**
     * 添加一个远程slave节点
     * @param slaveNodePeer
     */
    public void addRemoteSlaveNode(SlaveNodePeer slaveNodePeer) {
        slaveNodePeerMap.put(slaveNodePeer.getNodeId(), slaveNodePeer);
    }

    public List<MasterNodePeer> getAllRemoteMasterNodes() {
        return new ArrayList<>(masterNodePeerMap.values());
    }

    public List<MasterNodePeer> getOtherControllerCandidate() {
        Integer selfNodeId = serverConfig.getNodeId();
        List<MasterNodePeer> allOtherControllerCandidates = new ArrayList<>();
        for (MasterNodePeer masterNode : masterNodePeerMap.values()) {
            if(!masterNode.getNodeId().equals(selfNodeId)
            && masterNode.getIsControllerCandidate()) {
                allOtherControllerCandidates.add(masterNode);
            }
        }
        return allOtherControllerCandidates;
    }

    public NodeManager() {
        this.serverConfig = GovernServerConfig.getInstance();
        ConfigurationParser configurationParser = ConfigurationParser.getInstance();
        List<NodeAddress> nodeInfoList = configurationParser.parseMasterNodeServers();
        this.masterNumInCluster = nodeInfoList.size();
    }

}
