package regexpress.hiveserde;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;
import regexpress.common.GroupResult;
import regexpress.common.TestResult;
import regexpress.common.TestResultType;
import regexpress.common.Tester;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Main implements Tester {

    private static Gson gson = new Gson();

    public TestResult testRegex(String configJsonString, String testJsonString) {
        RegexSerDe regexSerde = new RegexSerDe();
        Type mapType = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> configJsonMap = gson.fromJson(configJsonString, mapType);

        Properties props = new Properties();
        props.setProperty(RegexSerDe.INPUT_REGEX, (String) configJsonMap.get(RegexSerDe.INPUT_REGEX));
        props.setProperty(serdeConstants.LIST_COLUMNS, (String) configJsonMap.get(serdeConstants.LIST_COLUMNS));
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, (String) configJsonMap.get(serdeConstants.LIST_COLUMN_TYPES));
        if(configJsonMap.containsKey(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE)) {
            props.setProperty(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE, (String) configJsonMap.get(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE));
        }
        if(configJsonMap.containsKey("output.format.string")) {
            props.setProperty("output.format.string", (String) configJsonMap.get("output.format.string"));
        }
        if(configJsonMap.containsKey("columns.comments")) {
            props.setProperty("columns.comments", (String) configJsonMap.get("columns.comments"));
        } else {
            String listColumns = props.getProperty(serdeConstants.LIST_COLUMNS);
            int columnCount = listColumns.split(",").length;
            String nullComments = "";
            for(int i = 0; i < columnCount - 1; i++) {
                nullComments += "\0";
            }
            props.setProperty("columns.comments", nullComments);
        }

        Type listType = new TypeToken<List<String>>(){}.getType();
        List<String> testStrings = gson.fromJson(testJsonString, listType);
        TestResult testResult = new TestResult();
        testResult.setResultType(TestResultType.GROUP);

        try {
            regexSerde.initialize(null, props);
            GroupResult groupResult = new GroupResult();
            for(int i = 0; i < testStrings.size(); i++) {
                String testString = testStrings.get(i);
                groupResult.getResultList().add((List<Object>) regexSerde.deserialize(new Text(testString)));
            }
            testResult.setTestResult(groupResult);
        } catch (SerDeException e) {
            e.printStackTrace();
        }

        return testResult;
    }

    public static void main(String args[]) {
        TestResult result = null;

        try {
            result = new Main().testRegex(args[0], args[1]);
        } catch (Exception e) {
            result = new TestResult();
            result.setException(e);
        }

        if(result != null) {
            System.out.println(gson.toJson(result));
        }
    }
}
