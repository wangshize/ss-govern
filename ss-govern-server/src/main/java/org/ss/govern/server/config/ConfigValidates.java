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

    private static final String MASTER_IP_PORT_REGEX = "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)\\:(\\d+)";

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
            throw new IllegalArgumentException("config master.node.servers must be master or slave");
        }
        String[] masterNodeServersArray = masterNodeServers.split(";");
        for (String masterNodeServer : masterNodeServersArray) {
            if (!Pattern.matches(MASTER_IP_PORT_REGEX, masterNodeServer)) {
                throw new IllegalArgumentException("config master.node.servers has a wrong pattern:" + masterNodeServer);
            }
        }
        return true;
    }

    public static String stringToMD5(String plainText) {
        byte[] secretBytes = null;
        try {
            secretBytes = MessageDigest.getInstance("md5").digest(
                    plainText.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("没有这个md5算法！");
        }
        String md5code = new BigInteger(1, secretBytes).toString(16);
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code = "0" + md5code;
        }
        return md5code;
    }

    public static String encodeURIComponent(String s) {
        String result = null;

        try {
            result = URLEncoder.encode(s, "UTF-8")
                    .replaceAll("\\+", "%20")
                    .replaceAll("\\%21", "!")
                    .replaceAll("\\%27", "'")
                    .replaceAll("\\%28", "(")
                    .replaceAll("\\%29", ")")
                    .replaceAll("\\%7E", "~");
        }

        // This exception should never occur.
        catch (UnsupportedEncodingException e) {
            result = s;
        }

        return result;
    }
}
