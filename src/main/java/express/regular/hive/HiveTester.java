package express.regular.hive;

import express.regular.common.GroupResult;
import express.regular.common.TestResult;
import express.regular.common.Tester;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HiveTester extends Tester {

    public static final String CONFIG_INPUT_REGEX = RegexSerDe.INPUT_REGEX;
    public static final String CONFIG_LIST_COLUMNS = serdeConstants.LIST_COLUMNS;
    public static final String CONFIG_LIST_COLUMN_TYPES = serdeConstants.LIST_COLUMN_TYPES;
    public static final String CONFIG_INPUT_REGEX_CASE_SENSITIVE = RegexSerDe.INPUT_REGEX_CASE_SENSITIVE;

    public TestResult testRegex(Map<String, Object> configMap, List<String> testStrings) throws Exception {
        RegexSerDe regexSerde = new RegexSerDe();

        Properties props = new Properties();
        props.setProperty(RegexSerDe.INPUT_REGEX, (String) configMap.get(CONFIG_INPUT_REGEX));
        props.setProperty(serdeConstants.LIST_COLUMNS, (String) configMap.get(CONFIG_LIST_COLUMNS));
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ((String) configMap.get(CONFIG_LIST_COLUMN_TYPES)).toLowerCase());
        if(configMap.containsKey(CONFIG_INPUT_REGEX_CASE_SENSITIVE)) {
            props.setProperty(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE, String.valueOf(configMap.get(CONFIG_INPUT_REGEX_CASE_SENSITIVE)));
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

        String columns[] = props.getProperty(serdeConstants.LIST_COLUMNS).split(",");

        regexSerde.initialize(null, props);

        GroupResult groupResult = new GroupResult();
        for(String column : columns) {
            groupResult.getColumns().add(column);
        }
        for(int i = 0; i < testStrings.size(); i++) {
            String testString = testStrings.get(i);
            try {
                ArrayList<Object> row = (ArrayList<Object>) regexSerde.deserialize(new Text(testString));
                if(row != null) {
                    GroupResult.GroupsList groupsList = new GroupResult.GroupsList((List<String>) row.clone());
                    groupResult.getResultList().add(groupsList);
                } else {
                    groupResult.getResultList().add(null);
                }

            } catch (SerDeException e) {
                throw new Exception(String.format("Error on %s", testString), e);
            }
        }
        testResult.setResult(groupResult);
        return testResult;
    }

    public static void main(String args[]) {
        Tester tester = new HiveTester();
        tester.testMain(args[0], args[1]);
    }
}
