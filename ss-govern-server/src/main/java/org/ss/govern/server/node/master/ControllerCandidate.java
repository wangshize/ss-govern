package org.ss.govern.server.node.master;

/**
 * controller候选人
 * @author wangsz
 * @create 2020-04-08
 **/
public class ControllerCandidate {

    private MasterNetworkManager masterNetworkManager;

    public ControllerCandidate(MasterNetworkManager masterNetworkManager) {
        this.masterNetworkManager = masterNetworkManager;
    }

    /**
     * 投票选举controller
     * @return
     */
    public int voteForControllerElection() {

        return 0;
    }
}
