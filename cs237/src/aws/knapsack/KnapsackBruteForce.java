package aws.knapsack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * KnapsackBruteForce uses a brute force way to find the optimal solution for the knapsack problem.
 * It splits the work load to multiple mappers by assigning each mapper to search a best solution
 * within a part of the solution space.
 *
 */
public class KnapsackBruteForce extends Configured implements Tool {

	public static final String INPUT_FILE_PATH_STRING = "inputFilePathString";
	public static final String SPLIT_FOLDER_PATH_STRING = "splitFolderPathString";
	public static final int NUMBER_OF_KEYS_TO_ENUMERATE = 3;

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

		private FileSystem fileSystem;
		private List<String> splitFileList;
		private List<Double> thresholdList;
		private List<String> knapsackList;
		private double minimumSumValue;
		private int leafLevel;

		public static final String INPUT_FILE_PATH_STRING = "inputFilePathString";
		public static final String SPLIT_FOLDER_PATH_STRING = "splitFolderPathString";
		public static final int NUMBER_OF_KEYS_TO_ENUMERATE = 3;

		@Override
		public void configure(JobConf jobConf) {
			BufferedReader inputBufferedReader = null;

			try {
				fileSystem = FileSystem.get(jobConf);
				splitFileList = new ArrayList<String>();
				thresholdList = new ArrayList<Double>();
				knapsackList = new ArrayList<String>();
				minimumSumValue = Double.MAX_VALUE;
				leafLevel = -1;

				inputBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(jobConf
						.get(INPUT_FILE_PATH_STRING)))));

				String inputKeyLine = inputBufferedReader.readLine();

				// initialize splitFileList with the split file locations, note the split0 file is not included,
				// because it is used as the input file of all the mappers
				if (inputKeyLine != null) {
					String[] keyArray = inputKeyLine.split("\t");

					for (int i = NUMBER_OF_KEYS_TO_ENUMERATE; i < keyArray.length; i++) {
						splitFileList.add(jobConf.get(SPLIT_FOLDER_PATH_STRING) + i + ".txt");
					}

					leafLevel = splitFileList.size() - 1;
				}

				String inputThresholdLine = inputBufferedReader.readLine();

				if (inputThresholdLine != null) {
					String[] thresholdArray = inputThresholdLine.split("\t");

					for (int i = 0; i < thresholdArray.length; i++) {
						thresholdList.add(Double.parseDouble(thresholdArray[i]));
					}
				}

			} catch (IOException e) {
				e.printStackTrace();

			} finally {
				try {
					if (inputBufferedReader != null) {
						inputBufferedReader.close();
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> outputCollector,
				Reporter reporter) throws IOException {

			// not necessary to start all over again, the previous value is actually good for the pruning,
			// but here still choose to do so to avoid ambiguity
			knapsackList.clear();
			minimumSumValue = Double.MAX_VALUE;

			String[] splitLineArray = value.toString().split("-");
			List<Double> valueList = new ArrayList<Double>();

			// knapsackPathList is the list which maintains the content of the knapsack
			List<String> knapsackPathList = new ArrayList<String>();

			// parse the enumeration line to form an initial knapsack
			for (int j = 0; j < splitLineArray.length; j++) {
				String[] splitArray = splitLineArray[j].split("\t");

				for (int i = 1; i < splitArray.length; i++) {
					if (j == 0) {
						valueList.add(Double.parseDouble(splitArray[i]));
					} else {
						valueList.set(i - 1, Double.parseDouble(splitArray[i]) + valueList.get(i - 1));
					}
				}

				knapsackPathList.add(splitLineArray[j]);
			}

			// if all split files have been combined into the enum file, don't need to dfs any more
			if (leafLevel != -1) {

				// use recursive method to search the solution space
				addNextLevelValue(0, valueList, knapsackPathList, reporter);

			} else {
				boolean allConstraintsSatisfied = true;
				double newSumValue = 0.0;

				for (int i = 0; i < valueList.size(); i++) {
					newSumValue += valueList.get(i);

					if (valueList.get(i) < thresholdList.get(i)) {
						allConstraintsSatisfied = false;
						break;
					}
				}

				if (allConstraintsSatisfied) {

					// compare the current result with the global optimal result,
					// if it's better, replace the global optimal result with it
					if (newSumValue < minimumSumValue) {
						minimumSumValue = newSumValue;
						knapsackList.clear();

						for (String knapsackPath : knapsackPathList) {
							knapsackList.add(new String(knapsackPath));
						}
					}
				}
			}

			if (!knapsackList.isEmpty()) {
				String knapsackListValue = "";

				for (int i = 0; i < knapsackList.size(); i++) {
					if (i != 0) {
						knapsackListValue += "-";
					}
					knapsackListValue += knapsackList.get(i);
				}

				// output the final minimum sum value and the string representation of the knapsack content
				outputCollector.collect(new DoubleWritable(minimumSumValue), new Text(knapsackListValue));
			}
		}

		/**
		 * a recursive method to depth first search the optimal solution for the knapsack problem
		 * 
		 * @param level different level has different key for the search method to find a object with 
		 * @param valueList maintains the values accumulated from the previous levels
		 * @param knapsackPathList maintains the knapsack content till the current level
		 */
		private void addNextLevelValue(int level, List<Double> valueList, List<String> knapsackPathList,
				Reporter reporter) {

			BufferedReader splitFileBufferedReader = null;
			String knapsackLine = null;

			try {
				// read the split file according to the current level
				splitFileBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(
						splitFileList.get(level)))));

				// test each candidate object from the split file
				while ((knapsackLine = splitFileBufferedReader.readLine()) != null) {
					String[] splitArray = knapsackLine.split("\t");
					List<Double> newValueList = new ArrayList<Double>();

					// reporter is needed to prevent from job timeout
					reporter.progress();

					// add the values of the current object with the previous values
					for (int i = 1; i < splitArray.length; i++) {
						newValueList.add(Double.parseDouble(splitArray[i]) + valueList.get(i - 1));
					}

					// if the leaf level is reached, test the constraints
					if (level == leafLevel) {
						boolean allConstraintsSatisfied = true;
						double newSumValue = 0.0;

						for (int i = 0; i < newValueList.size(); i++) {
							newSumValue += newValueList.get(i);

							if (newValueList.get(i) < thresholdList.get(i)) {
								allConstraintsSatisfied = false;
								break;
							}
						}

						if (allConstraintsSatisfied) {
							if (newSumValue < minimumSumValue) {
								minimumSumValue = newSumValue;
								knapsackList.clear();

								for (String knapsackPath : knapsackPathList) {
									knapsackList.add(new String(knapsackPath));
								}
								knapsackList.add(new String(knapsackLine));
							}
						}

					} else {

						// if it is not the leaf level, calculate the current result also
						double currentSumValue = 0.0;

						for (int i = 0; i < newValueList.size(); i++) {
							currentSumValue += newValueList.get(i);
						}

						// prune the branch of the search tree, if the current result is
						// already bigger than the global optimal result
						if (currentSumValue < minimumSumValue) {
							knapsackPathList.add(knapsackLine);

							// start the next level of search
							addNextLevelValue(level + 1, newValueList, knapsackPathList, reporter);

							knapsackPathList.remove(knapsackLine);
						}
					}
				}

			} catch (IOException ioe) {
				ioe.printStackTrace();

			} finally {
				try {
					if (splitFileBufferedReader != null) {
						splitFileBufferedReader.close();
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		private boolean isTheFirstOne = true;

		public void reduce(DoubleWritable key, Iterator<Text> values,
				OutputCollector<DoubleWritable, Text> outputCollector, Reporter reporter) throws IOException {

			// just output the first data received from mapper, this reduce class is needed to ensure a sorted output
			if (isTheFirstOne && values.hasNext()) {
				isTheFirstOne = false;

				outputCollector.collect(key, values.next());
			}
		}
	}

	public int run(String[] args) throws Exception {

		// the compiled jar can be deployed on both local hadoop and AWS EMR hadoop,
		// the difference between these two is local hadoop doesn't need any arguments while AWS EMR hadoop needs 4
		boolean usingAws = args.length == 4;

		// all internal file paths are hard coded for convenience
		String inputFolderPathString = "input";
		String datasetFilePathString = inputFolderPathString + "/knapsack_dataset.txt";
		String inputFilePathString = inputFolderPathString + "/knapsack_input.txt";
		String outputFolderPathString = "bf_output";
		String splitFolderPathString = outputFolderPathString + "/knapsack_split";
		String enumFilePathString = outputFolderPathString + "/knapsack_enum.txt";
		String bfFolderPathString = outputFolderPathString + "/knapsack_brute_force";
		String outputFilePathString = outputFolderPathString + "/knapsack_bf_output.txt";

		// if AWS is used, copy the dataset and input files from s3 to hdfs
		if (usingAws) {
			String fileSystemUriString = args[0]; // s3n://knapsack/
			String datasetPathString = args[1]; // input/knapsack_dataset.txt
			String inputPathString = args[2]; // input/knapsack_input.txt

			FileSystem s3FileSystem = FileSystem.get(URI.create(fileSystemUriString), getConf());
			FileSystem hdfsFileSystem = FileSystem.get(getConf());

			BufferedReader s3BufferedReader = new BufferedReader(new InputStreamReader(s3FileSystem.open(new Path(
					fileSystemUriString + datasetPathString))));
			PrintWriter hdfsPrintWriter = new PrintWriter(hdfsFileSystem.create(new Path(datasetFilePathString)));
			String s3ReadLine = null;

			while ((s3ReadLine = s3BufferedReader.readLine()) != null) {
				hdfsPrintWriter.println(s3ReadLine);
			}

			s3BufferedReader.close();
			hdfsPrintWriter.close();

			s3BufferedReader = new BufferedReader(new InputStreamReader(s3FileSystem.open(new Path(fileSystemUriString
					+ inputPathString))));
			hdfsPrintWriter = new PrintWriter(hdfsFileSystem.create(new Path(inputFilePathString)));

			while ((s3ReadLine = s3BufferedReader.readLine()) != null) {
				hdfsPrintWriter.println(s3ReadLine);
			}

			s3BufferedReader.close();
			hdfsPrintWriter.close();
		}

		// this is a job pipeline with two jobs
		JobControl jobControl = new JobControl("DatasetSpliter");

		JobConf datasetSpliterJobConf = new JobConf(DatasetSpliter.class);
		datasetSpliterJobConf.setJobName("DatasetSpliter");
		datasetSpliterJobConf.set(INPUT_FILE_PATH_STRING, inputFilePathString);
		datasetSpliterJobConf.set(SPLIT_FOLDER_PATH_STRING, splitFolderPathString);

		datasetSpliterJobConf.setOutputKeyClass(Text.class);
		datasetSpliterJobConf.setOutputValueClass(Text.class);

		datasetSpliterJobConf.setMapperClass(DatasetSpliter.Map.class);
		datasetSpliterJobConf.setReducerClass(DatasetSpliter.Reduce.class);

		datasetSpliterJobConf.setInputFormat(TextInputFormat.class);
		datasetSpliterJobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(datasetSpliterJobConf, datasetFilePathString);
		FileOutputFormat.setOutputPath(datasetSpliterJobConf, new Path(splitFolderPathString));

		// the first job is to split the data
		Job datasetSpliterJob = new Job(datasetSpliterJobConf);

		jobControl.addJob(datasetSpliterJob);

		new Thread(jobControl).start();

		while (!jobControl.allFinished()) {
			Thread.sleep(2000);
		}

		jobControl.stop();

		FileSystem fileSystem = FileSystem.get(getConf());

		BufferedReader splitFileBufferedReader = null;
		String splitFileLine = null;
		List<List<String>> enumerationList = new ArrayList<List<String>>();

		// generate a file which contains the full enumeration of the objects within the first several
		// split files, and use this file as the input of the next job, this step is aimed to reduce
		// the recursion depth of the brute force search and allow more mappers to divide the solution space
		for (int i = 0; i < NUMBER_OF_KEYS_TO_ENUMERATE; i++) {
			Path splitFilePath = new Path(splitFolderPathString + i + ".txt");

			if (fileSystem.exists(splitFilePath)) {
				splitFileBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(splitFilePath)));
				List<String> splitFileLineList = new ArrayList<String>();

				while ((splitFileLine = splitFileBufferedReader.readLine()) != null) {
					splitFileLineList.add(splitFileLine);
				}

				enumerationList.add(splitFileLineList);

				splitFileBufferedReader.close();

			} else {
				break;
			}
		}

		PrintWriter enumFilePrintWriter = new PrintWriter(fileSystem.create(new Path(enumFilePathString)));

		outputEnumerationFile(enumerationList, enumFilePrintWriter, "", 0);

		enumFilePrintWriter.close();

		jobControl = new JobControl("KnapsackBruteForce");

		JobConf bruteForceJobConf = new JobConf(KnapsackBruteForce.class);
		bruteForceJobConf.setJobName("KnapsackBruteForce");
		bruteForceJobConf.set(INPUT_FILE_PATH_STRING, inputFilePathString);
		bruteForceJobConf.set(SPLIT_FOLDER_PATH_STRING, splitFolderPathString);

		bruteForceJobConf.setOutputKeyClass(DoubleWritable.class);
		bruteForceJobConf.setOutputValueClass(Text.class);

		bruteForceJobConf.setMapperClass(KnapsackBruteForce.Map.class);
		bruteForceJobConf.setReducerClass(KnapsackBruteForce.Reduce.class);
		bruteForceJobConf.setNumReduceTasks(1);

		bruteForceJobConf.setInputFormat(TextInputFormat.class);
		bruteForceJobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(bruteForceJobConf, enumFilePathString);
		FileOutputFormat.setOutputPath(bruteForceJobConf, new Path(bfFolderPathString));

		// the second job is to do the brute force search
		Job bruteForceJob = new Job(bruteForceJobConf);

		jobControl.addJob(bruteForceJob);

		new Thread(jobControl).start();

		while (!jobControl.allFinished()) {
			Thread.sleep(2000);
		}

		jobControl.stop();

		Path bruteForceResultPath = new Path(bfFolderPathString + "/part-00000");

		if (fileSystem.exists(bruteForceResultPath)) {
			BufferedReader bruteForceBufferedReader = new BufferedReader(new InputStreamReader(fileSystem
					.open(bruteForceResultPath)));
			PrintWriter bruteForcePrintWriter = null;
			String bruteForceResult = null;

			// if AWS is used, set the output writer to use the s3 file system
			if (usingAws) {
				String fileSystemUriString = args[0]; // s3n://knapsack/
				String outputPathString = args[3]; // sa_output/knapsack_sa_output.txt

				bruteForcePrintWriter = new PrintWriter(FileSystem.get(URI.create(fileSystemUriString), getConf())
						.create(new Path(fileSystemUriString + outputPathString)));

			} else {
				bruteForcePrintWriter = new PrintWriter(fileSystem.create(new Path(outputFilePathString)));
			}

			// in the end, select the first result of the brute force search and output it as the final solution
			if ((bruteForceResult = bruteForceBufferedReader.readLine()) != null) {
				int resultIndex = bruteForceResult.indexOf("\t");
				bruteForcePrintWriter.println(String.format("%.2f", Double.parseDouble(bruteForceResult
						.substring(0, resultIndex))));

				String[] knapsackArray = bruteForceResult.substring(resultIndex + 1).split("-");
				for (String knapsackLine : knapsackArray) {
					bruteForcePrintWriter.println(knapsackLine);
				}
			}

			bruteForceBufferedReader.close();
			bruteForcePrintWriter.close();
		}

		return 0;
	}

	/**
	 * a recursive method to print out the full enumeration of objects with the first several keys
	 * 
	 * @param enumerationList stores several lists of the objects with the same keys
	 * @param enumFilePrintWriter is passed to output the enumeration lines in the end
	 * @param enumLine the current enumeration representation
	 * @param level
	 */
	private static void outputEnumerationFile(List<List<String>> enumerationList, PrintWriter enumFilePrintWriter,
			String enumLine, int level) {

		if (level == enumerationList.size()) {
			enumFilePrintWriter.println(enumLine);

		} else {
			for (int i = 0; i < enumerationList.get(level).size(); i++) {
				String newEnumLine = null;

				if (level == 0) {
					newEnumLine = enumLine + enumerationList.get(level).get(i);
				} else {
					newEnumLine = enumLine + "-" + enumerationList.get(level).get(i);
				}

				outputEnumerationFile(enumerationList, enumFilePrintWriter, newEnumLine, level + 1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new KnapsackBruteForce(), args));
	}
}
