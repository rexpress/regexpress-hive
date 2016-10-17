package express.regular.hive.test;

import com.google.gson.Gson;
import express.regular.common.GroupResult;
import express.regular.common.TestResult;
import express.regular.hive.HiveTester;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveTesterTest {

    @Test
    public void hiveTest() {
        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(RegexSerDe.INPUT_REGEX, "([a-zA-Z]*) ([a-zA-Z]*) ([a-zA-Z]*)");
        configMap.put(serdeConstants.LIST_COLUMNS, "A,B,C");
        configMap.put(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");
        configMap.put(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE, "true");
//        configMap.put("output.format.string", "");
//        configMap.put("columns.comments", null);

        List<String> testMap = Arrays.asList(new String[]{"Hello Test String", "Hello2 Test2 String2"});

        Gson gson = new Gson();
        String configJsonString = gson.toJson(configMap);
        String testJsonString = gson.toJson(testMap);

        HiveTester tester = new HiveTester();
        TestResult testResult = tester.testMain(configJsonString, testJsonString);
        Assert.assertEquals(testResult.getType(), TestResult.Type.GROUP);
        Assert.assertNotNull(testResult.getResult());
        GroupResult groupResult = (GroupResult) testResult.getResult();
        Assert.assertArrayEquals(groupResult.getColumns().toArray(), new String[]{"A", "B", "C"});
        Assert.assertArrayEquals(groupResult.getResultList().get(0).toArray(), new String[]{"Hello", "Test", "String"});
        Assert.assertNull(groupResult.getResultList().get(1));
    }
}