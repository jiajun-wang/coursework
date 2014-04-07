package aws.knapsack;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * KnapsackSorting takes advantage of hadoop to sort the output of the knapsack algorithm by the
 * minimum sum value. This will help to select the best solution of the last batch process of the
 * knapsack program.
 *
 */
public class KnapsackSorting {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> outputCollector,
				Reporter reporter) throws IOException {

			int resultIndex = value.toString().indexOf("\t");

			// the key of the output is the minimum sum value calculated by the last map-reduce job
			outputCollector.collect(new DoubleWritable(Double.parseDouble(value.toString().substring(0, resultIndex))),
									new Text(value.toString().substring(resultIndex + 1)));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		private boolean isTheFirstOne = true;

		public void reduce(DoubleWritable key, Iterator<Text> values,
				OutputCollector<DoubleWritable, Text> outputCollector, Reporter reporter) throws IOException {

			// only need to output the first data because the shuffle process of hadoop has already sorted the data by key
			if (isTheFirstOne && values.hasNext()) {
				isTheFirstOne = false;

				outputCollector.collect(key, values.next());
			}
		}
	}
}
