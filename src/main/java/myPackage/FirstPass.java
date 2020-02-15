package myPackage;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class FirstPass {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        // Read the csv file and put the second field and the seventh field in a tuple
//        DataSet<Tuple2<String, Float>> csvInput = env.readCsvFile(params.get("input")).includeFields(0100001000)
        DataSet<Tuple2<String, Float>> csvInput = env.readCsvFile("openaq_kabul.csv").includeFields(010000100000)
                .types(String.class, Float.class);


        csvInput.print();

        env.execute("OpenAq Job 1");

    }
}
