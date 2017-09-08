package DataProcess.Batch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 * <p>Usage: <code> WordCount --input /path/to/some/text/data --output /path/to/result

 */
@SuppressWarnings("serial")
public class BatchWordCountDemo {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> text = env.readTextFile("/Users/nanazhu/Documents/Sapienza/pg1787.txt");
		// TODO how to read from Cassandra

        // split up the lines in pairs (2-tuples) containing: (word,1) group by the tuple field "0" and sum up tuple field "1"
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.print();
//        counts.writeAsCsv("/Users/nanazhu/Documents/Sapienza/pg1787.csv", "\n", " ");

	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

}
