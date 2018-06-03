package com.utils.es;


import com.utils.es.client.Response;
import com.utils.es.client.RestClient;
import com.utils.es.client.RestClientBuilder;
import com.utils.es.client.sniff.Sniffer;
import com.utils.es.json.JsonTool;
import com.utils.es.model.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.AbstractHttpAsyncClient;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class RestEsManager {

    public static final Logger LOGGER = LogManager.getLogger(RestEsManager.class);
    //连接对象
    private RestClient client;
    //嗅探
    private Sniffer sniffer;
    //单例
    private volatile static ConcurrentHashMap<String, RestEsManager> utils = new ConcurrentHashMap<String, RestEsManager>();

    private RestEsManager(String hosts, int port, String user, String password, boolean sniff) {
        //认证信息
        user = user == null ? "" : user;
        password = password == null ? "" : password;
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        //处理数据,添加主机信息
        hosts = hosts.trim();
        if (hosts.endsWith(",")) {
            hosts = hosts.substring(0, hosts.length() - 1);
        }
        String host_array[] = hosts.split(",");
        HttpHost[] httphosts = new HttpHost[host_array.length];
        for (int i = 0; i < host_array.length; i++) {
            String host = host_array[i];
            httphosts[i] = new HttpHost(host, port, "http");
        }
        //建立连接
        this.client = RestClient.builder(httphosts)
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    //设置线程数
                    @Override
                    public IOReactorConfig customizeRequestConfig(IOReactorConfig ioReactorConfig) {
                        ioReactorConfig.setIoThreadCount(10);
                        return ioReactorConfig;
                    }
                })
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    //设置认证
                    @Override
                    public AbstractHttpAsyncClient customizeHttpClient(AbstractHttpAsyncClient httpClientBuilder) {
                        httpClientBuilder.setCredentialsProvider(credentialsProvider);
                        return httpClientBuilder;
                    }
                })
                .setMaxRetryTimeoutMillis(30000)
                .build();
        //初始化嗅探
        if (sniff) {
            this.sniffer = Sniffer.builder(this.client).build();
        }
    }

    /**
     * 单例
     *
     * @return 单例
     * @throws Exception
     */
    public static RestEsManager getInstance() {
        return getInstance("localhost", 9200, false);
    }

    /**
     * 单例
     *
     * @return 单例
     * @throws Exception
     */
    public static RestEsManager getInstance(String hosts, int port, boolean sniff) {
        return getInstance(hosts, port, null, null, sniff);
    }

    /**
     * 单例
     *
     * @return 单例
     * @throws Exception
     */
    public static RestEsManager getInstance(String hosts, int port, String user, String password, boolean sniff) {
        String util_key = hosts + port + sniff;
        if (!utils.containsKey(util_key)) {
            synchronized (RestEsManager.class) {
                if (!utils.containsKey(util_key)) {
                    utils.put(util_key, new RestEsManager(hosts, port, user, password, sniff));
                }
            }
        }
        return utils.get(util_key);
    }

    @Override
    protected void finalize() throws Throwable {
        //嗅探需要先关闭【生命周期要短】
        if (sniffer != null) {
            sniffer.close();
        }
        //后关闭连接
        if (client != null) {
            client.close();
        }
    }

    /**
     * 批量存储数据
     *
     * @param list 待存储的数据
     * @return 是否保存成功
     */
    public boolean bulkLoad(List<IndexSource> list) {
        if (list != null && list.size() > 0) {
            //拼装请求
            StringBuffer sbf = new StringBuffer();
            for (IndexSource item : list) {
                if (item != null && item.getIndex() != null && item.getIndex().ok()) {
                    //判断【es5.0 sourc 不支持 _id】
                    IndexModel indexModel = item.getIndex();
                    SourceModel sourceModel = item.getSource();
                    if (sourceModel == null) {
                        sourceModel = new SourceModel();
                    }
                    if (sourceModel.containsKey("_id")) {
                        sourceModel.remove("_id");
                    }
                    //拼装json数据
                    Map<String, IndexModel> index = new HashMap<String, IndexModel>();
                    index.put("index", indexModel);
                    sbf.append(JsonTool.beanToJson(index) + "\n");
                    sbf.append(JsonTool.beanToJson(sourceModel) + "\n");
                } else {
                    LOGGER.warn("数据存在异常，这将跳过执行。");
                }
            }
            //提交请求
            boolean flag = false;
            try {
                //提交请求
                HttpEntity entity = new NStringEntity(sbf.toString(), ContentType.APPLICATION_JSON);
                Response response = client.performRequest(HttpPost.METHOD_NAME, "/_bulk", Collections.<String, String>emptyMap(), entity);
                //获取状态
                if (response.getStatusLine().getStatusCode() == 200) {
                    String responseBody = EntityUtils.toString(response.getEntity());
                    if (JsonTool.parseJson(responseBody, "errors").asBoolean(false)) {
                        //这里存在索引错误的情况
                        LOGGER.error(responseBody);
                    } else {
                        flag = true;
                    }
                }
            } catch (IOException e) {
                LOGGER.error("es插入数据异常：", e);
            }
            //返回
            return flag;
        } else {
            return true;
        }
    }

    public boolean bulkLoadOne(IndexSource indexSource) {
        StringBuffer sbf = new StringBuffer();
        if (indexSource != null && indexSource.getIndex() != null && indexSource.getIndex().ok()) {
            //判断【es5.0 sourc 不支持 _id】
            IndexModel indexModel = indexSource.getIndex();
            SourceModel sourceModel = indexSource.getSource();
            if (sourceModel == null) {
                sourceModel = new SourceModel();
            }
            if (sourceModel.containsKey("_id")) {
                sourceModel.remove("_id");
            }
            //拼装json数据
            Map<String, IndexModel> index = new HashMap<String, IndexModel>();
            index.put("index", indexModel);
            sbf.append(JsonTool.beanToJson(index) + "\n");
            sbf.append(JsonTool.beanToJson(sourceModel) + "\n");
        }
        boolean flag = false;
        try {
            //提交请求
            HttpEntity entity = new NStringEntity(sbf.toString(), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(HttpPost.METHOD_NAME, "/_bulk", Collections.<String, String>emptyMap(), entity);
            //获取状态
            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                if (JsonTool.parseJson(responseBody, "errors").asBoolean(false)) {
                    //这里存在索引错误的情况
                    LOGGER.error(responseBody);
                } else {
                    flag = true;
                }
            }
        } catch (IOException e) {
            LOGGER.error("es插入数据异常：", e);
        }
        //返回
        return flag;
    }


    /**
     * 查询数据，不存在则返回nulll
     *
     * @param indexModel 查询
     * @return 返回数据，不存在则返回nulll
     */
    public Map<String, Object> getById(IndexModel indexModel) {
        if (indexModel.ok()) {
            try {
                //发送请求
                Response response = client.performRequest(HttpGet.METHOD_NAME, indexModel.toUri());
                if (response.getStatusLine().getStatusCode() == 200) {
                    //解析数据
                    String res = EntityUtils.toString(response.getEntity());
                    GetModel get = JsonTool.JsonToEntity(res, GetModel.class);
                    Map<String, Object> map = get.get_source();
                    map.put("_id", get.get_id());
                    //返回
                    return map;
                }
            } catch (IOException e) {
                LOGGER.error("es查询数据异常：", e);
            }
        } else {
            LOGGER.error("查询非法,这里将忽略错误：" + indexModel.toString());
        }
        return null;
    }

    /**
     * 获取mapping信息
     *
     * @param index
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMapping(String index) throws IOException {
        Response response = client.performRequest(HttpGet.METHOD_NAME, "/_mappings");
        if (response.getStatusLine().getStatusCode() == 200) {
            //解析数据
            String res = EntityUtils.toString(response.getEntity());
            return JsonTool.JsonToEntity(res, Map.class);
        }
        return null;
    }


    /**
     * 创建es 的索引
     *
     * @param index
     * @return 是否创建成功
     * @throws IOException
     */
    public boolean CreateIndex(String index) throws IOException {
        Response response = client.performRequest(HttpPut.METHOD_NAME, "/" + index);
        if (response.getStatusLine().getStatusCode() == 200) {
            LOGGER.info("create one index and name is :" + index);
            return true;
        }
        return false;
    }

    /**
     * 创建es的mapping结构
     *
     * @param indexModel 需要在哪个index下面创建
     * @param indexModel 创建时指定的type
     * @param mappings   mapping的字符串，一般用jsontool将对象转换过来。
     * @return
     * @throws IOException
     */
    public boolean Create_mapping(IndexModel indexModel, String mappings) throws IOException {

        String end = null;
        if (indexModel.ok()) {
            indexModel.set_id("_mapping");
            end = indexModel.toUri();
        } else {
            LOGGER.info("缺少index信息或者type信息");
            return false;
        }
        HttpEntity entity = new NStringEntity(mappings, ContentType.APPLICATION_JSON);
        Response response = client.performRequest(HttpPut.METHOD_NAME, end, Collections.<String, String>emptyMap(), entity);
        if (response.getStatusLine().getStatusCode() == 200) {
            LOGGER.info("create mappings of index and type  :" + indexModel.get_index() + " type" + indexModel.get_type());
            return true;
        }
        return false;

    }

    /**
     * 根据id对索引数据进行删除操作。
     *
     * @param indexModel 需要删除的id对应的模型，构建模型时候，一定要将id set进去
     * @return 删除成功返回TRUE，否则返回FALSE。
     * @throws IOException
     */
    public boolean delete_id(IndexModel indexModel) throws IOException {
        Map<String, Object> byId = this.getById(indexModel);
        if (byId != null) {
            //存在这个id的数据，需要进行删除操作
            String end = indexModel.toUri();
            Response response = client.performRequest(HttpDelete.METHOD_NAME, end);
            if (response.getStatusLine().getStatusCode() == 200) {
                LOGGER.info("delete data by id and indexmodel :" + indexModel.toString());
                String responseBody = EntityUtils.toString(response.getEntity());
                try {
                    DelModel delModel=JsonTool.JsonToEntity(responseBody,DelModel.class);
                    return "deleted".equals(delModel.getResult());
                }catch (Exception e){
                    LOGGER.error("delete "+end+e);
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * 批量删除数据，根据id进行删除操作。
     * 其实批量操作的时候，可以将插入和删除放在一起。此次不做方法的封装。
     *
     * @param indexModels 里面构建模型的时候，一定要将id进行set进去。
     * @return
     */
    public boolean delete_id(List<IndexModel> indexModels) {
        StringBuffer sbf = new StringBuffer();
        for (IndexModel indexModel : indexModels) {
            Map<String, IndexModel> index = new HashMap<String, IndexModel>();
            index.put("delete", indexModel);
            sbf.append(JsonTool.beanToJson(index)).append("\n");
        }
        boolean flag = false;
        try {
            //提交请求
            System.out.println(sbf.toString());
            HttpEntity entity = new NStringEntity(sbf.toString(), ContentType.APPLICATION_JSON);
            //批量操作的时候使用put.
            Response response = client.performRequest(HttpPut.METHOD_NAME, "/_bulk", Collections.<String, String>emptyMap(), entity);
            //获取状态
            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                if (JsonTool.parseJson(responseBody, "errors").asBoolean(false)) {
                    //这里存在索引错误的情况
                    LOGGER.error(responseBody);
                } else {
                    flag = true;
                }
            }
        } catch (IOException e) {
            LOGGER.error("es删除数据异常：", e);
        }
        //返回
        return flag;

    }


    /**
     * 将返回的字符串构建为list类型数据返回。为了查询方法抽出来的
     *
     * @param responseBody 返回的字符串
     * @return 抽出来的结果。
     */
    private List<Map<String, Object>> str_to_list(String responseBody) {
        List<Map<String, Object>> datas = new ArrayList<>();
        Map<String, Object> map1 = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        List<Map<String, Object>> list_map3 = new ArrayList<>();
        //将结果转为map
        map1 = JsonTool.JsonToEntity(responseBody, map1.getClass());

        //获取到hits
        map2 = JsonTool.JsonToEntity(JsonTool.beanToJson(map1.get("hits")), map2.getClass());
        //hits中结果
        list_map3 = JsonTool.JsonToEntity(JsonTool.beanToJson(map2.get("hits")), list_map3.getClass());
        //遍历取出结果
        for (Map<String, Object> data : list_map3) {
            Map<String, Object> result = new HashMap<>();
            result = JsonTool.JsonToEntity(JsonTool.beanToJson(data.get("_source")), result.getClass());
            datas.add(result);
        }
        //返回结果
        return datas;
    }

    /**
     * 构建一些通用的设置
     *
     * @param felids 返回字段
     * @param sort   排序字段
     * @param asc    是否升序
     * @param from   多少条开始返回
     * @param limit  返回多少条
     * @return
     */
    private Map<String, Object> to_map(List<String> felids, String sort, boolean asc, Integer from, Integer limit) {
        Map<String, Object> query = new HashMap<>();
        if (from != null) {
            query.put("from", from);
        } else {
            query.put("from", 0);
        }
        if (limit != null) {
            query.put("size", limit);
        } else {
            query.put("size", 10);
        }
        if (sort != null && !sort.isEmpty()) {
            Map<String, Map<String, String>> sort_desc = new HashMap<>();
            Map<String, String> desc = new HashMap<>();
            //排序设置为降序
            if (asc) {
                desc.put("order", "asc");
            } else {
                desc.put("order", "desc");
            }
            sort_desc.put(sort, desc);
            query.put("sort", sort_desc);
        }
        //设定返回的字段
        if (felids != null && felids.size() > 0) {
            query.put("_source", felids);
        }
        return query;

    }

    /**
     * 从所有的数据中查询数据，匹配所有的模式
     *
     * @param indexModel index相关信息
     * @param felids     返回的字段
     * @param sort       排序字段
     * @param from       从第几个返回
     * @param limit      返回个数
     * @return
     */
    public List<Map<String, Object>> search_match_all(IndexModel indexModel, List<String> felids, String sort, boolean asc, Integer from, Integer limit) {
        indexModel.set_id("_search");
        String end = indexModel.toUri();
        //获取通用的一些设置
        Map<String, Object> query = this.to_map(felids, sort, asc, from, limit);
        //构建当前条件下的一些设置
        Map<String, Object> match_all = new HashMap<>();
        match_all.put("match_all", new HashMap<>());
        query.put("query", match_all);

        try {
            HttpEntity entity = new NStringEntity(JsonTool.beanToJson(query), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(HttpPost.METHOD_NAME, end, Collections.<String, String>emptyMap(), entity);
            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                return this.str_to_list(responseBody);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }


    /**
     * 按照单字段条件进行查询。
     *
     * @param indexModel
     * @param key        查询所对应的key
     * @param val        查询key所对应的值。
     * @param felids     返回字段
     * @param sort       排序
     * @param from       多少条开始返回
     * @param limit      返回多少条
     * @return
     */
    public List<Map<String, Object>> search_match(IndexModel indexModel, String key, Object val, List<String> felids, String sort, boolean asc, Integer from, Integer limit) {
        indexModel.set_id("_search");
        String end = indexModel.toUri();
        //获取通用的设置
        Map<String, Object> query = this.to_map(felids, sort, asc, from, limit);

        //构建当前的查询条件
        Map<String, Object> match = new HashMap<>();
        Map<String, Object> key_val = new HashMap<>();
        Map<String, Object> ands = new HashMap<>();
        //构建精确查找，需要添加and关键字。
        ands.put("query", val);
        ands.put("operator", "and");
        //构建好的添加到查找字段中去
        key_val.put(key, ands);
        //将字段查找添加到match中
        match.put("match", key_val);
        //构建成完整的查找条件语句
        query.put("query", match);
        try {
            HttpEntity entity = new NStringEntity(JsonTool.beanToJson(query), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(HttpPost.METHOD_NAME, end, Collections.<String, String>emptyMap(), entity);
            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                return this.str_to_list(responseBody);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 构建针对字段进行范围查找
     *
     * @param indexModel
     * @param key        查找字段
     * @param val1       最小值
     * @param val2       最大值
     * @param isequls    是否包含相等
     * @param felids
     * @param sort
     * @param asc        TRUE为升序 FALSE为降序
     * @param from
     * @param limit
     * @return
     */
    public List<Map<String, Object>> search_match(IndexModel indexModel, String key, Object val1, Object val2, boolean isequls, List<String> felids, String sort, boolean asc, Integer from, Integer limit) {
        indexModel.set_id("_search");
        String end = indexModel.toUri();
        Map<String, Object> query = this.to_map(felids, sort, asc, from, limit);

        Map<String, Object> range = new HashMap<>();
        Map<String, Object> key_val = new HashMap<>();
        Map<String, Object> min_max = new HashMap<>();
        //构建精确查找，需要添加and关键字。
        if (isequls) {
            //包含等于的情况
            min_max.put("gte", val1);
            min_max.put("lte", val2);
        } else {
            //不包含等于
            min_max.put("gt", val1);
            min_max.put("lt", val2);
        }
        //构建好的添加到查找字段中去
        key_val.put(key, min_max);
        //将字段查找添加到match中
        range.put("range", key_val);
        //构建成完整的查找条件语句
        query.put("query", range);
        try {
            HttpEntity entity = new NStringEntity(JsonTool.beanToJson(query), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(HttpPost.METHOD_NAME, end, Collections.<String, String>emptyMap(), entity);
            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                return this.str_to_list(responseBody);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 多条件的复杂查询
     * @param indexModel
     * @param must_filter 同时满足的条件   key value
     * @param must  是否打分查询  TRUE 打分 FALSE 不打分
     * @param must_not  排除条件
     * @param should    满足条件之一
     * @param felids
     * @param sort
     * @param asc
     * @param from
     * @param limit
     * @return
     */
    public List<Map<String, Object>> search(IndexModel indexModel, List<Map<String, Object>> must_filter,boolean must, List<Map<String, Object>> must_not, List<Map<String, Object>> should, List<String> felids, String sort, boolean asc, Integer from, Integer limit) {
        indexModel.set_id("_search");
        String end = indexModel.toUri();
        Map<String, Object> query = this.to_map(felids, sort, asc, from, limit);
        //构建查询的条件
        Map<String, Object> bool = new HashMap<>();
        //添加must的条件
        if (must_filter != null && must_filter.size() > 0) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Map<String, Object> key_val : must_filter) {
               for(Map.Entry<String,Object> entity:key_val.entrySet()){
                   Map<String, Object> term = new HashMap<>();
                   Map<String, Object> term_value = new HashMap<>();
                   term_value.put(entity.getKey(),entity.getValue());
                   term.put("term",term_value);
                   list.add(term);
               }
            }
            if(must){
                bool.put("must", list);
            }else{
                bool.put("filter", list);
            }

        } else if (must_filter != null && must_filter.size() == 0) {
            if(must){
                bool.put("must", must_filter);
            }else{
                bool.put("filter", must_filter);
            }
        }
        //添加must_not的条件
        if (must_not != null && must_not.size() > 0) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Map<String, Object> key_val : must_not) {
                for(Map.Entry<String,Object> entity:key_val.entrySet()){
                    Map<String, Object> term = new HashMap<>();
                    Map<String, Object> term_value = new HashMap<>();
                    term_value.put(entity.getKey(),entity.getValue());
                    term.put("term",term_value);
                    list.add(term);
                }
            }
            bool.put("must_not", list);
        } else if (must_not != null && must_not.size() == 0) {
            bool.put("must_not", must_not);
        }

        if (should != null && should.size() > 0) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Map<String, Object> key_val : should) {
                for(Map.Entry<String,Object> entity:key_val.entrySet()){
                    Map<String, Object> term = new HashMap<>();
                    Map<String, Object> term_value = new HashMap<>();
                    term_value.put(entity.getKey(),entity.getValue());
                    term.put("term",term_value);
                    list.add(term);
                }
            }
            bool.put("should", list);
        } else if (should != null && should.size() == 0) {
            bool.put("should", should);
        }

        Map<String,Object> bool_query=new HashMap<>();
        bool_query.put("bool", bool);
        query.put("query",bool_query);

        System.out.println(JsonTool.beanToJson(query));
        try {
            HttpEntity entity = new NStringEntity(JsonTool.beanToJson(query), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(HttpPost.METHOD_NAME, end, Collections.<String, String>emptyMap(), entity);
            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                return this.str_to_list(responseBody);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) throws IOException {
        IndexModel indexModel=new IndexModel("test-index","index","ae15a911-d097-4fd2-87b6-eea968a86ace");
        System.out.println(RestEsManager.getInstance().delete_id(indexModel));
    }
}
