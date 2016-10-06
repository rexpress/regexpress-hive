package regexpress.hiveserde.test;

import com.google.gson.Gson;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.junit.Test;
import regexpress.common.TestResult;
import regexpress.hiveserde.Main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainTest {
    private static Gson gson = new Gson();
    @Test
    public void mainTest() {
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

        Main main = new Main();
        TestResult testResult = main.testRegex(configJsonString, testJsonString);

        if(testResult != null) {
            System.out.println(gson.toJson(testResult));
        }

        /*testResult.getTestResult().forEach((result)->{
            System.out.println(result);
        });*/

    }
}