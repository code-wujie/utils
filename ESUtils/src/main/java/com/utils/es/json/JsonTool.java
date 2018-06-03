package com.utils.es.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author diven
 */
public class JsonTool {

    private static Logger logger = LogManager.getLogger(JsonTool.class);

    /**
     * 将Bean转化为json字符串
     * @param obj bean对象
     * @return json
     */
    public static String beanToJson(Object obj) {
        String json = null;
        try {
            return new ObjectMapper().writeValueAsString(obj);
        } catch (IOException e) {
            logger.error("beanToJson fail!", e);
        }
        return json;
    }

    /**
     * 获取Set
     * @param jsonStr 需要转化的json字符串
     * @param elementClasse 元素类
     * @return List Java List
     * @since 1.0
     */
    @SuppressWarnings("unchecked")
    public static <T> Set<T> jsonToSet(String jsonStr, Class<T> elementClasse) {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.getTypeFactory().constructParametricType(HashSet.class, elementClasse);
        Set<T> lst = null;
        try {
            lst = (Set<T>) mapper.readValue(jsonStr, javaType);
        } catch (JsonParseException e) {
            logger.error("JsonParseException occur jsonToSet fail!", e);
        } catch (JsonMappingException e) {
            logger.error("JsonMappingException occur jsonToSet fail!", e);
        } catch (IOException e) {
            logger.error("IOException occur jsonToSet fail!", e);
        }
        return lst;
    }

    /**
     * 获取List
     * @param jsonStr 需要转化的json字符串
     * @param elementClasse 元素类
     * @return List Java List
     * @since 1.0
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> jsonToList(String jsonStr, Class<T> elementClasse) {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, elementClasse);
        List<T> lst = null;
        try {
            lst = (List<T>) mapper.readValue(jsonStr, javaType);
        } catch (JsonParseException e) {
            logger.error("JsonParseException occur jsonToList fail!", e);
        } catch (JsonMappingException e) {
            logger.error("JsonParseException occur jsonToList fail!", e);
        } catch (IOException e) {
            logger.error("JsonParseException occur jsonToList fail!", e);
        }
        return lst;
    }

    /**
     * 获取Set
     * @param jsonStr 需要转化的json字符串
     * @param collectionClass 泛型的Collection
     * @param elementClasses 元素类
     * @return List Java List
     * @since 1.0
     */
    public static HashSet<?> jsonToSet(String jsonStr, Class<?> collectionClass, Class<?>... elementClasses) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JavaType javaType = mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
            return (HashSet<?>) mapper.readValue(jsonStr, javaType);
        }catch (Exception e) {
            logger.error("Exception occur jsonToSet fail!", e);
            return null;
        }
    }

    /**
     * 获取List
     * @param jsonStr 需要转化的json字符串
     * @param collectionClass 泛型的Collection
     * @param elementClasses 元素类
     * @return List Java List
     * @since 1.0
     */
    public static <T> T jsonToList(String jsonStr, Class<?> collectionClass, Class<?>... elementClasses) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JavaType javaType = mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
            return mapper.readValue(jsonStr, javaType);
        }catch (Exception e) {
            logger.error("Exception occur jsonToList fail!", e);
            return null;
        }
    }

    /**
     * 将字符串转换为Entity
     * @param json 数据字符串
     * @param clazz	Entity class
     * @return
     */
    public static <T> T JsonToEntity(String json, Class<T> clazz) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Object o = mapper.readValue(json, clazz);
            return clazz.cast(o);
        }catch (Exception e) {
            logger.error("Exception occur JsonToEntity fail!", e);
            return null;
        }
    }

    /**
     * 解析json字符串
     * @param jsonStr  原始字符串
     * @param regs 解析规则
     * @return 返回结果
     */
    public static JsonNode parseJson(String jsonStr, String... regs) {
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(jsonStr);
            for (String reg : regs) {
                jsonNode = jsonNode.path(reg);
            }
        } catch (IOException e) {
            logger.error("IOException occur parseJson fail!", e);
        }
        return jsonNode;
    }

    /**
     * 格式化数据
     * @param obj 数据
     * @return	字符串
     */
    public static String format(Object obj) {
        try {
            if(obj instanceof String){
                try {
                    obj = new ObjectMapper().readTree((String)obj);
                }
                catch (IOException e) {
                    logger.error("IOException occur format fail!", e);
                }
            }
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException occur format fail!", e);
            return null;
        }
    }

}
