package org.ss.govern.utils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 配置合法性校验
 *
 * @author wangsz
 * @create 2020-04-05
 **/
public class ConfigValidates {

    private static String NODE_TYPE_MASTER = "master";
    private static String NODE_TYPE_SLAVE = "slave";

    public static boolean checkNodeRole(String nodeRole) {
        return NODE_TYPE_MASTER.equals(nodeRole) || NODE_TYPE_SLAVE.equals(nodeRole);
    }

    public static void main(String[] args) throws Exception {
        String answer = "不美不美";
        String encode = URLEncoder.encode(answer, "utf-8");
        String v = "91650103MA77TY8B85" + encode;
        String v2 = "91650103MA77TY8B85%25E4%25B8%258D%25E7%25BE%258E";
        v2 = encodeURIComponent(v);
        System.out.println(stringToMD5(v2));

        String v3 = "" + "" + "123456" + "123456" + URLEncoder.encode("我美不美", "utf-8") + URLEncoder.encode("美", "utf-8");
        System.out.println(stringToMD5(encodeURIComponent(v3)));
        String v4 = "123456" + "123456" + URLEncoder.encode("我美不美", "utf-8") + URLEncoder.encode("美", "utf-8");
        System.out.println(stringToMD5(encodeURIComponent(v4)));
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
