package aws.knapsack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * DatasetSpliter accepts the dataset and input files from hdfs, extracts the candidate object data
 * with the keys assigned by the input file, and splits the result into different split files.
 *
 */
public class DatasetSpliter {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private FileSystem fileSystem;
		private String[] inputKeyArray;

		public static final String INPUT_FILE_PATH_STRING = "inputFilePathString";

		@Override
		public void configure(JobConf jobConf) {
			try {
				fileSystem = FileSystem.get(jobConf);

				// read the input file
				BufferedReader inputKeyBufferReader = new BufferedReader(new InputStreamReader(fileSystem
						.open(new Path(jobConf.get(INPUT_FILE_PATH_STRING)))));
				String inputKeyLine = inputKeyBufferReader.readLine();

				if (inputKeyLine != null) {
					inputKeyArray = inputKeyLine.split("\t");
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter)
				throws IOException {

			String[] splitArray = value.toString().split("\t");
			int inputKeyIndex = -1;

			// check whether the key of the candidate object is the one specified by the input file
			for (int i = 0; i < inputKeyArray.length; i++) {
				if (inputKeyArray[i].equals(splitArray[0])) {
					inputKeyIndex = i;
					break;
				}
			}

			// output key, index of key and values
			if (inputKeyIndex != -1) {
				String valueLine = splitArray[1];
				for (int i = 2; i < splitArray.length; i++) {
					valueLine += "-" + splitArray[i];
				}

				outputCollector.collect(new Text(inputKeyIndex + "-" + splitArray[0]), new Text(valueLine));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

		private FileSystem fileSystem;
		private String splitFolderPathString;

		public static final String SPLIT_FOLDER_PATH_STRING = "splitFolderPathString";

		@Override
		public void configure(JobConf jobConf) {
			try {
				fileSystem = FileSystem.get(jobConf);

				splitFolderPathString = jobConf.get(SPLIT_FOLDER_PATH_STRING);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> outputCollector,
				Reporter reporter) throws IOException {

			PrintWriter splitPrintWriter = null;

			try {
				String[] keyArray = key.toString().split("-");
				Path path = new Path(splitFolderPathString + keyArray[0] + ".txt");
				splitPrintWriter = new PrintWriter(fileSystem.exists(path) ? fileSystem.append(path) : fileSystem
						.create(path));
				int valueLineCount = 0;

				// output the collection of values with the same key to one split file
				while (values.hasNext()) {
					splitPrintWriter.print(keyArray[1]);

					String[] valueArray = ((Text) values.next()).toString().split("-");
					for (String value : valueArray) {
						splitPrintWriter.print("\t" + value);
					}

					splitPrintWriter.println();
					valueLineCount++;
				}

				// output the number of candidate objects with each key to the default output file
				outputCollector.collect(key, new IntWritable(valueLineCount));

			} catch (IOException ioe) {
				ioe.printStackTrace();

			} finally {
				if (splitPrintWriter != null) {
					splitPrintWriter.close();
				}
			}
		}
	}
}
