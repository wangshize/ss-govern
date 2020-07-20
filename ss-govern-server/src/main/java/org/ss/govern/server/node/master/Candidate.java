package org.ss.govern.server.node.master;

import org.ss.govern.server.node.MessageReceiver;
import org.ss.govern.server.node.NetworkManager;

/**
 * @author wangsz
 * @create 2020-07-09
 **/
public class Candidate {

    private MessageReceiver messageReceiver;

    private NetworkManager networkManager;

    public Candidate(MessageReceiver messageReceiver, NetworkManager networkManager) {
        this.messageReceiver = messageReceiver;
        this.networkManager = networkManager;
    }

}
