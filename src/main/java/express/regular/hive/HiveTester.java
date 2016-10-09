package express.regular.hive;

import com.google.gson.Gson;
import express.regular.common.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HiveTester extends Tester {

    private static Gson gson = new Gson();

    public static final String CONFIG_INPUT_REGEX = RegexSerDe.INPUT_REGEX;
    public static final String CONFIG_LIST_COLUMNS = serdeConstants.LIST_COLUMNS;
    public static final String CONFIG_LIST_COLUMN_TYPES = serdeConstants.LIST_COLUMN_TYPES;
    public static final String CONFIG_INPUT_REGEX_CASE_SENSITIVE = RegexSerDe.INPUT_REGEX_CASE_SENSITIVE;

    public TestResult testRegex(Map<String, Object> configMap, List<String> testStrings) {
        RegexSerDe regexSerde = new RegexSerDe();

        Properties props = new Properties();
        props.setProperty(RegexSerDe.INPUT_REGEX, (String) configMap.get(CONFIG_INPUT_REGEX));
        props.setProperty(serdeConstants.LIST_COLUMNS, (String) configMap.get(CONFIG_LIST_COLUMNS));
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ((String) configMap.get(CONFIG_LIST_COLUMN_TYPES)).toLowerCase());
        if(configMap.containsKey(CONFIG_INPUT_REGEX_CASE_SENSITIVE)) {
            props.setProperty(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE, (String) configMap.get(CONFIG_INPUT_REGEX_CASE_SENSITIVE));
        }
        if(configMap.containsKey("output.format.string")) {
            props.setProperty("output.format.string", (String) configMap.get("output.format.string"));
        }
        if(configMap.containsKey("columns.comments")) {
            props.setProperty("columns.comments", (String) configMap.get("columns.comments"));
        } else {
            String columns = props.getProperty(serdeConstants.LIST_COLUMNS);
            int columnCount = columns.split(",").length;
            String nullComments = "";
            for(int i = 0; i < columnCount - 1; i++) {
                nullComments += "\0";
            }
            props.setProperty("columns.comments", nullComments);
        }

        TestResult testResult = new TestResult();
        testResult.setType(TestResult.Type.GROUP);

        try {
            regexSerde.initialize(null, props);
            String columns[] = props.getProperty(serdeConstants.LIST_COLUMNS).split(",");

            GroupResult groupResult = new GroupResult();
            for(String column : columns) {
                groupResult.getColumns().add(column);
            }
            for(int i = 0; i < testStrings.size(); i++) {
                String testString = testStrings.get(i);
                groupResult.getResultList().add((List<Object>) regexSerde.deserialize(new Text(testString)));
            }
            testResult.setResult(groupResult);
        } catch (SerDeException e) {
            e.printStackTrace();
        }

        return testResult;
    }

    public static void main(String args[]) {
        Tester tester = new HiveTester();
        tester.testMain(args[0], args[1]);
    }
}
