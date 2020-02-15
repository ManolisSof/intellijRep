package myPackage;


import org.apache.flink.api.common.functions.MapFunction;
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
        // Read the file and create a tuple with 3 attributes: City_Name, Gas_Name, Measurement
//        DataSet<Tuple3<String, String, Float>> csvInput = env.readTextFile("openaq_kabul.csv")
//                .map(new MapFunction<String, Tuple3<String, String, Float>>() {
//            @Override
//            public Tuple3<String, String, Float> map(String value) throws Exception {
//                String[] words = value.split(",");
//                Tuple3<String, String, Float> t1 = new Tuple3<>(words[1], words[5], Float.parseFloat(words[6]));
//                return t1;
//            }
//        });

        // Read the file and create a tuple with 2 attributes: City_Name, Measurement
        DataSet<Tuple2<String, Float>> csvInput = env.readTextFile("openaq.csv")
                .map(new MapFunction<String, Tuple2<String, Float>>() {
                    @Override
                    public Tuple2<String, Float> map(String value) throws Exception {
                        String[] words = value.split(",");
                        Tuple2<String, Float> t1 = new Tuple2<>(words[1], Float.parseFloat(words[6]));
                        return t1;
                    }
                });

        DataSet<Tuple2<String, Float>> sum = csvInput.groupBy(new int[] {0}).sum(1);

        sum.print();

        env.execute("OpenAq Job 1");

    }
}
