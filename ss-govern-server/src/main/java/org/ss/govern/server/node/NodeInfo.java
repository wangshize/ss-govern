package org.ss.govern.server.node;

/**
 * @author wangsz
 * @create 2020-04-10
 **/
public class NodeInfo {

    /**
     * 节点ID
     */
    private Integer nodeId;

    /**
     * 是否参与controller选举
     */
    private Boolean isControllerCandidate;

    /**
     * 节点ip
     */
    private String ip;

    /**
     * master节点间通信的端口
     */
    private Integer masterConnectPort;

    /**
     * master与slave节点间通信的端口
     */
    private Integer slaveConnectPort;

    /**
     * master与客户端间通信的端口
     */
    private Integer clientConnectPort;

    public NodeInfo(Integer nodeId, String ip, Integer masterConnectPort,
                    Integer slaveConnectPort, Integer clientConnectPort) {
        this.nodeId = nodeId;
        this.ip = ip;
        this.masterConnectPort = masterConnectPort;
        this.slaveConnectPort = slaveConnectPort;
        this.clientConnectPort = clientConnectPort;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public String getIp() {
        return ip;
    }

    public Boolean getControllerCandidate() {
        return isControllerCandidate;
    }

    public void setControllerCandidate(Boolean controllerCandidate) {
        isControllerCandidate = controllerCandidate;
    }

    public Integer getMasterConnectPort() {
        return masterConnectPort;
    }

    public Integer getSlaveConnectPort() {
        return slaveConnectPort;
    }

    public Integer getClientConnectPort() {
        return clientConnectPort;
    }
}
