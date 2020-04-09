package org.ss.govern.server.config;

import org.ss.govern.core.constants.NodeRole;
import org.ss.govern.utils.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;

/**
 * 配置合法性校验
 *
 * @author wangsz
 * @create 2020-04-05
 **/
public class ConfigValidates {

    private static final String MASTER_IP_PORT_REGEX = "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)\\:(\\d+)\\:(\\d+)";
    private static final String NODE_ID_REGEX = "(\\d+)";

    public static boolean checkNodeRole(String nodeRole) {
        if (StringUtils.isNotEmpty(nodeRole)) {
            if (!(NodeRole.MASTER.equals(nodeRole)
                    || NodeRole.SLAVE.equals(nodeRole))) {
                throw new IllegalArgumentException("config node.type must be master or slave");
            }
        } else {
            throw new IllegalArgumentException("config node.role can not be null");
        }
        return true;
    }

    public static boolean checkMasterNodeServers(String masterNodeServers) {
        if (StringUtils.isEmpty(masterNodeServers)) {
            throw new IllegalArgumentException("config master.node.servers can not be empty");
        }
        String[] masterNodeServersArray = masterNodeServers.split(";");
        for (String masterNodeServer : masterNodeServersArray) {
            boolean isMatch = Pattern.matches(MASTER_IP_PORT_REGEX, masterNodeServer);
            if (!isMatch) {
                throw new IllegalArgumentException("config master.node.servers has a wrong pattern:" + masterNodeServer);
            }
        }
        return true;
    }

    public static boolean checkNodeId(String nodeId) {
        if(StringUtils.isEmpty(nodeId)) {
            throw new IllegalArgumentException("config node.id can not be empty");
        }
        boolean isMatch = Pattern.matches(NODE_ID_REGEX, nodeId);
        if(!isMatch) {
            throw new IllegalArgumentException("node.id must be a number");
        }
        return isMatch;
    }
}
