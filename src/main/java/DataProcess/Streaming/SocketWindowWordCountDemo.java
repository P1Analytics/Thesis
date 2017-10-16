package DataProcess.Streaming;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
 * Implements a streaming windowed version of the "WordCount" program.
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * open Terminal /  command : nc -l 9000 / terminate terminal
 * and run this example with the hostname and the port as arguments.
 */

@SuppressWarnings("serial")
public class SocketWindowWordCountDemo {

	public static void main(String[] args) throws Exception {
		// the host and the port to connect to
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = Integer.parseInt(params.has("port") ? params.get("port") : "9000");
//			port = 9000;
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
				"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
				"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
				"type the input text into the command line");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = text
				.flatMap(
						new FlatMapFunction<String, WordWithCount>() {
						public void flatMap(String value, Collector<WordWithCount> out) 
						{
						for (String word : value.split("\\s")) {
							out.collect(new WordWithCount(word, 1L));}
							}
						})
				.keyBy("word")
				.timeWindow(Time.seconds(5))
				.reduce(new ReduceFunction<WordWithCount>() {
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);
		env.execute("Socket Window WordCount");
	}

	public static class WordWithCount {

		public String word;
		public long count;
		public WordWithCount() {}
		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}
        public String toString() {
			return word + " : " + count;
		}
	}
}
