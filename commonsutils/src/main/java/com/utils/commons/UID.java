package com.utils.commons;

import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Created by WJ on 2018/6/6.
 */
public class UID {

    /**
     * 将字符串转为md5,64位的。
     * @param s
     * @return
     */
    public static String getMD5(String s){
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            BASE64Encoder base64en = new BASE64Encoder();
            return base64en.encode(md.digest(s.getBytes("utf-8")));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "";
    }


    /**
     * 产生一个随机的长36位的字符串
     * @return
     */
    public static String getUUID(){
        return UUID.randomUUID().toString();
    }


}
