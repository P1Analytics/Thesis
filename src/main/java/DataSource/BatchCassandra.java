package DataSource;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class BatchCassandra {
    public static void main(String[] args) throws IOException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ClusterBuilder cb = new ClusterBuilder() {
            @Override
            public Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("127.0.0.1").build();
            }
        };
        String query = "SELECT ResourceID,Reading FROM gaia.reading_data WHERE ResourceID=155873";
        InputFormat<Tuple2<Integer, Float>, InputSplit> source = new CassandraInputFormat<>(query, cb);
        source.configure(null);
        source.open(null);
        List<Tuple2<Integer, Float>> result = new ArrayList<>();
        while (!source.reachedEnd()) result.add(source.nextRecord(new Tuple2<Integer, Float>()));
        source.close();

        DataSource<List<Tuple2<Integer, Float>>> value = env.fromElements(result);
        DataSet<Tuple2<String, Integer>> counts = value.flatMap(new Tokenizer()).groupBy(0).sum(1);
        try {
            counts.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Tokenizer implements FlatMapFunction<List<Tuple2<Integer, Float>>, Tuple2<String, Integer>> {
        public void flatMap(List<Tuple2<Integer, Float>> input, Collector<Tuple2<String, Integer>> output) {
            List<Tuple2<Integer, Float>> tokens = input;
            for (Tuple2<Integer, Float> token : tokens) {
                output.collect(new Tuple2<String, Integer>(token.f0.toString(), 1));
                output.collect(new Tuple2<String, Integer>(token.f1.toString(), 1));
            }
        }
    }


}
