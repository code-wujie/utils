package com.utils.hive.udf;

import com.wujie.util.JsonUtils;
import org.apache.hadoop.hive.ql.exec.UDF;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by WJ on 2018/6/5.
 */
public class StrToMap extends UDF {
    public Map<String,Object> evaluate(String str) {
        try {
            return JsonUtils.JsonToMap(str);
        }catch (Exception e){
            return null;
        }

    }

}
