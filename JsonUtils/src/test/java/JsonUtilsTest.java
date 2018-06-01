import com.wujie.util.JsonUtils;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.List;
import java.util.Map;

/**
 * JsonUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>���� 31, 2018</pre>
 */
public class JsonUtilsTest {

    Stu stu;
    String json;
    @Before
    public void before() throws Exception {
        this.stu=new Stu();
        stu.setName("test");
        stu.setAge(5);

        this.json="{\"age\":10,\"name\":\"testandtest\"}";
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: BeanToJsonStr(Object obj)
     */
    @Test
    public void testBeanToJsonStr() throws Exception {
//TODO: Test goes here...
        System.out.println(JsonUtils.BeanToJsonStr(this.stu));
    }

    /**
     * Method: JsonToBean(String json, Class<T> clazz)
     */
    @Test
    public void testJsonToBean() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: JsonToList(String json, Class<T> clazz)
     */
    @Test
    public void testJsonToList() throws Exception {
//TODO: Test goes here...
        String s="["+this.json+","+JsonUtils.BeanToJsonStr(this.stu)+"]";
        System.out.println(s);
        List<? extends Stu> stus = JsonUtils.JsonToList(s, this.stu.getClass());
        System.out.println(stus);
    }

    /**
     * Method: JsonToMap(String json)
     */
    @Test
    public void testJsonToMapJson() throws Exception {
//TODO: Test goes here...

    }

    /**
     * Method: JsonToMap(String json, Class<T> clazz)
     */
    @Test
    public void testJsonToMapForJsonClazz() throws Exception {
//TODO: Test goes here...
        String s="{\"one\":{\"age\":10,\"name\":\"testandtest\"}}";
        Map<String, Object> stringObjectMap = JsonUtils.JsonToMap(s);
        Map<String, ? extends Stu> stringMap = JsonUtils.JsonToMap(s, this.stu.getClass());
        System.out.println(stringObjectMap);
        System.out.println(stringMap);

    }

    public static class Stu{
        String name;
        int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Stu{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

}
