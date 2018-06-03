package com.wujie.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.List;
import java.util.Map;

/**
 * Created by WJ on 2018/5/31.
 */
public class JsonUtils {


    public static boolean BeanToJsonAsBool(Object o){
        try {
            JSON.toJSONString(o);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 将对象转为json字符串
     *
     * @param obj 要解析的对象
     * @return 解析后的字符串
     */
    public static String BeanToJsonStr(Object obj) {
        return JSON.toJSONString(obj);
    }

    /**
     * 将string转为指定的对象
     *
     * @param json  字符串
     * @param clazz 需要转成的对象的类
     * @return 对象
     */
    public static <T> T JsonToBean(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    /**
     * 将string转为list
     *
     * @param json  字符串
     * @param clazz list内部的对象的类型
     * @param <T>
     * @return list结构，且每一个元素都是指定类型
     */
    public static <T> List<T> JsonToList(String json, Class<T> clazz) {
        return JSON.parseArray(json, clazz);
    }

    /**
     * 将json转为map，key为string，value为object
     *
     * @param json 字符串
     * @return Map<String,Object>
     */
    public static Map<String, Object> JsonToMap(String json) {
        return JSON.parseObject(json, Map.class);
    }

    /**
     * 返回一个map，key为string，value为泛型
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> Map<String, T> JsonToMap(String json, Class<T> clazz) {
        return JSON.parseObject(json, new TypeReference<Map<String, T>>() {
        });
    }
}
