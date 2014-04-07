package aws.knapsack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
 * KnapsackSimulatedAnnealing uses hadoop to run simulated annealing algorithm to solve the knapsack
 * problem. There are multiple reducers running the annealing method in parallel to get a solution.
 * All solutions will be gathered and compared in the end. And the best solution will be selected.
 *
 */
public class KnapsackSimulatedAnnealing extends Configured implements Tool {

	public static final String INPUT_FILE_PATH_STRING = "inputFilePathString";
	public static final String SPLIT_FOLDER_PATH_STRING = "splitFolderPathString";

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> outputCollector,
				Reporter reporter) throws IOException {

			// output eleven random keys for each key specified, this map class is needed to run a number of reducers
			for (int i = 0; i < 11; i++) {
				outputCollector.collect(new IntWritable(new Random().nextInt(100)), value);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, DoubleWritable, Text> {

		private FileSystem fileSystem;
		private List<String> splitFileList;
		private List<Double> thresholdList;
		private java.util.Map<Integer, Integer> valueCountMap;
		private List<String> knapsackList;
		private double minimumSumValue;

		public static final String INPUT_FILE_PATH_STRING = "inputFilePathString";
		public static final String SPLIT_FOLDER_PATH_STRING = "splitFolderPathString";

		@Override
		public void configure(JobConf jobConf) {
			BufferedReader inputBufferedReader = null;

			try {
				fileSystem = FileSystem.get(jobConf);
				splitFileList = new ArrayList<String>();
				thresholdList = new ArrayList<Double>();
				valueCountMap = new HashMap<Integer, Integer>();
				knapsackList = new ArrayList<String>();

				inputBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(jobConf
						.get(INPUT_FILE_PATH_STRING)))));

				String inputKeyLine = inputBufferedReader.readLine();

				// initialize splitFileList with the split file locations
				if (inputKeyLine != null) {
					String[] keyArray = inputKeyLine.split("\t");

					for (int i = 0; i < keyArray.length; i++) {
						splitFileList.add(jobConf.get(SPLIT_FOLDER_PATH_STRING) + i + ".txt");
					}
				}

				String inputThresholdLine = inputBufferedReader.readLine();

				if (inputThresholdLine != null) {
					String[] thresholdArray = inputThresholdLine.split("\t");

					for (int i = 0; i < thresholdArray.length; i++) {
						thresholdList.add(Double.parseDouble(thresholdArray[i]));
					}
				}

				inputBufferedReader.close();

				for (FileStatus fileStatus : fileSystem.listStatus(new Path(jobConf.get(SPLIT_FOLDER_PATH_STRING)))) {
					if (fileStatus.getPath().getName().startsWith("part-")) {
						inputBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus
								.getPath())));
						String valueCountLine = null;

						// valueCountMap is used to find the number of objects with a certain key
						while ((valueCountLine = inputBufferedReader.readLine()) != null) {
							String[] valueCountArray = valueCountLine.split("\t");

							valueCountMap.put(Integer.parseInt(valueCountArray[0].split("-")[0]), Integer
									.parseInt(valueCountArray[1]));
						}

						inputBufferedReader.close();
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

		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<DoubleWritable, Text> outputCollector, Reporter reporter) throws IOException {

			// every reducer does its own annealing process independently
			anneal(1000.0, 0.999, 0.0001, reporter);

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
		 * regular simulated annealing algorithm implementation
		 * 
		 * @param temperature
		 * @param coolingRate
		 * @param absoluteTemperature
		 */
		private void anneal(double temperature, double coolingRate, double absoluteTemperature, Reporter reporter) {
			double deltaMinimumSumValue = 0.0;
			Random random = new Random();
			List<String> tempKnapsackList = new ArrayList<String>();

			// generate an initial solution
			double tempMinimumSumValue = generateInitialSolution(tempKnapsackList);

			while (temperature > absoluteTemperature) {
				List<String> nextKnapsackList = new ArrayList<String>();

				// reporter is needed to prevent from task timeout
				reporter.progress();

				// generate a neighboring solution
				double nextMinimumSumValue = generateNextSolution(tempKnapsackList, nextKnapsackList);

				deltaMinimumSumValue = nextMinimumSumValue - tempMinimumSumValue;

				if ((deltaMinimumSumValue <= 0)
						|| (deltaMinimumSumValue > 0 && Math.exp(-deltaMinimumSumValue / temperature) > random
								.nextDouble())) {

					tempMinimumSumValue = nextMinimumSumValue;
					tempKnapsackList.clear();
					for (String knapsackLine : nextKnapsackList) {
						tempKnapsackList.add(knapsackLine);
					}
				}

				temperature *= coolingRate;
			}

			// after all the iterations, store the final result
			minimumSumValue = tempMinimumSumValue;
			knapsackList.clear();
			for (String knapsackLine : tempKnapsackList) {
				knapsackList.add(knapsackLine);
			}
		}

		/**
		 * generate an initial solution for the knapsack problem by selecting the candidate objects randomly
		 * 
		 * @param tempKnapsackList the output parameter which is used to return the generated knapsack content
		 * @return the minimum sum value of the generated knapsack content
		 */
		private double generateInitialSolution(List<String> tempKnapsackList) {
			BufferedReader splitFileBufferedReader = null;
			String knapsackLine = null;
			Random random = new Random();
			double[] valueArray = null;
			boolean allConstraintsSatisfied = false;
			double tempMinimumSumValue = 0.0;

			while (!allConstraintsSatisfied) {
				for (int i = 0; i < splitFileList.size(); i++) {
					try {
						splitFileBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(
								splitFileList.get(i)))));

						// for each key, go to the corresponding split file and select an object randomly
						for (int randomInt = random.nextInt(valueCountMap.get(i)); randomInt > 0; randomInt--) {
							splitFileBufferedReader.readLine();
						}

						if ((knapsackLine = splitFileBufferedReader.readLine()) != null) {
							String[] splitArray = knapsackLine.split("\t");

							if (valueArray == null) {
								valueArray = new double[splitArray.length - 1];
							}

							// update valueArray for future constraint checking
							for (int j = 1; j < splitArray.length; j++) {
								valueArray[j - 1] += Double.parseDouble(splitArray[j]);
							}

							tempKnapsackList.add(knapsackLine);
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

				tempMinimumSumValue = 0.0;
				allConstraintsSatisfied = true;

				// calculate the constraints and minimum sum value
				for (int i = 0; i < valueArray.length; i++) {
					tempMinimumSumValue += valueArray[i];

					// if one of the constraints is not met, abandon this result and redo the whole method
					if (valueArray[i] < thresholdList.get(i)) {
						allConstraintsSatisfied = false;
						tempMinimumSumValue = 0.0;
						tempKnapsackList.clear();
						valueArray = null;

						break;
					}
				}
			}

			return tempMinimumSumValue;
		}

		/**
		 * generate a neighboring solution for the knapsack problem by randomly changing candidate
		 * object of one of the keys
		 * 
		 * @param tempKnapsackList the input parameter which shows the old knapsack content
		 * @param nextKnapsackList the output parameter which is used to return the new knapsack content
		 * @return the minimum sum value of the new knapsack content
		 */
		private double generateNextSolution(List<String> tempKnapsackList, List<String> nextKnapsackList) {
			BufferedReader splitFileBufferedReader = null;
			String knapsackLine = null;
			Random random = new Random();
			double[] valueArray = null;
			boolean allConstraintsSatisfied = false;
			double nextMinimumSumValue = 0.0;

			// randomly select a key to change an object with
			int randomFileIndex = random.nextInt(splitFileList.size());

			while (!allConstraintsSatisfied) {
				try {
					splitFileBufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(
							splitFileList.get(randomFileIndex)))));

					// for the randomly selected key, go to the corresponding split file and select an object randomly
					for (int randomInt = random.nextInt(valueCountMap.get(randomFileIndex)); randomInt > 0; randomInt--) {
						splitFileBufferedReader.readLine();
					}

					if ((knapsackLine = splitFileBufferedReader.readLine()) != null) {
						String[] splitArray = knapsackLine.split("\t");

						valueArray = new double[splitArray.length - 1];

						// update valueArray for future constraint checking
						for (int j = 1; j < splitArray.length; j++) {
							for (int k = 0; k < tempKnapsackList.size(); k++) {
								if (k != randomFileIndex) {
									String[] tempSplitArray = tempKnapsackList.get(k).split("\t");
									valueArray[j - 1] += Double.parseDouble(tempSplitArray[j]);
								}
							}

							valueArray[j - 1] += Double.parseDouble(splitArray[j]);
						}

						// generate a new knapsack with the old knapsack content and the new object
						for (int i = 0; i < tempKnapsackList.size(); i++) {
							if (i == randomFileIndex) {
								nextKnapsackList.add(knapsackLine);
							} else {
								nextKnapsackList.add(tempKnapsackList.get(i));
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

				nextMinimumSumValue = 0.0;
				allConstraintsSatisfied = true;

				// calculate the constraints and minimum sum value
				for (int i = 0; i < valueArray.length; i++) {
					nextMinimumSumValue += valueArray[i];

					// if one of the constraints is not met, abandon this result and redo the whole method
					if (valueArray[i] < thresholdList.get(i)) {
						allConstraintsSatisfied = false;
						nextMinimumSumValue = 0.0;
						nextKnapsackList.clear();
						valueArray = null;

						break;
					}
				}
			}

			return nextMinimumSumValue;
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
		String outputFolderPathString = "sa_output";
		String splitFolderPathString = outputFolderPathString + "/knapsack_split";
		String saFolderPathString = outputFolderPathString + "/knapsack_simulated_annealing";
		String sortingFolderPathString = outputFolderPathString + "/knapsack_sorting";
		String outputFilePathString = outputFolderPathString + "/knapsack_sa_output.txt";

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

		// this is a job pipeline with three jobs
		JobControl jobControl = new JobControl("KnapsackSimulatedAnnealing");

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

		JobConf simulatedAnnealingJobConf = new JobConf(KnapsackSimulatedAnnealing.class);
		simulatedAnnealingJobConf.setJobName("KnapsackSimulatedAnnealing");
		simulatedAnnealingJobConf.set(INPUT_FILE_PATH_STRING, inputFilePathString);
		simulatedAnnealingJobConf.set(SPLIT_FOLDER_PATH_STRING, splitFolderPathString);

		simulatedAnnealingJobConf.setOutputKeyClass(IntWritable.class);
		simulatedAnnealingJobConf.setOutputValueClass(Text.class);

		simulatedAnnealingJobConf.setMapperClass(KnapsackSimulatedAnnealing.Map.class);
		simulatedAnnealingJobConf.setReducerClass(KnapsackSimulatedAnnealing.Reduce.class);

		simulatedAnnealingJobConf.setInputFormat(TextInputFormat.class);
		simulatedAnnealingJobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(simulatedAnnealingJobConf, new Path(splitFolderPathString));
		FileOutputFormat.setOutputPath(simulatedAnnealingJobConf, new Path(saFolderPathString));

		// the second job is to do the simulated annealing in parallel
		Job simulatedAnnealingJob = new Job(simulatedAnnealingJobConf);
		simulatedAnnealingJob.addDependingJob(datasetSpliterJob);

		jobControl.addJob(simulatedAnnealingJob);

		JobConf sortingJobConf = new JobConf(KnapsackSorting.class);
		sortingJobConf.setJobName("KnapsackSorting");

		sortingJobConf.setOutputKeyClass(DoubleWritable.class);
		sortingJobConf.setOutputValueClass(Text.class);

		sortingJobConf.setMapperClass(KnapsackSorting.Map.class);
		sortingJobConf.setReducerClass(KnapsackSorting.Reduce.class);
		sortingJobConf.setNumReduceTasks(1);

		sortingJobConf.setInputFormat(TextInputFormat.class);
		sortingJobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(sortingJobConf, new Path(saFolderPathString));
		FileOutputFormat.setOutputPath(sortingJobConf, new Path(sortingFolderPathString));

		// the third job is to sort all the simulated annealing solutions
		Job sortingJob = new Job(sortingJobConf);
		sortingJob.addDependingJob(simulatedAnnealingJob);

		jobControl.addJob(sortingJob);

		new Thread(jobControl).start();

		while (!jobControl.allFinished()) {
			Thread.sleep(2000);
		}

		jobControl.stop();

		FileSystem fileSystem = FileSystem.get(getConf());
		Path sortingResultPath = new Path(sortingFolderPathString + "/part-00000");

		if (fileSystem.exists(sortingResultPath)) {
			BufferedReader simulatedAnnealingBufferedReader = new BufferedReader(new InputStreamReader(fileSystem
					.open(sortingResultPath)));
			PrintWriter simulatedAnnealingPrintWriter = null;
			String simulatedAnnealingResult = null;

			// if AWS is used, set the output writer to use the s3 file system
			if (usingAws) {
				String fileSystemUriString = args[0]; // s3n://knapsack/
				String outputPathString = args[3]; // sa_output/knapsack_sa_output.txt

				simulatedAnnealingPrintWriter = new PrintWriter(FileSystem.get(URI.create(fileSystemUriString),
																				getConf())
						.create(new Path(fileSystemUriString + outputPathString)));

			} else {
				simulatedAnnealingPrintWriter = new PrintWriter(fileSystem.create(new Path(outputFilePathString)));
			}

			// in the end, select the first result of the sorting job and output it as the final solution
			if ((simulatedAnnealingResult = simulatedAnnealingBufferedReader.readLine()) != null) {
				int resultIndex = simulatedAnnealingResult.indexOf("\t");
				simulatedAnnealingPrintWriter.println(String.format("%.2f", Double.parseDouble(simulatedAnnealingResult
						.substring(0, resultIndex))));

				String[] knapsackArray = simulatedAnnealingResult.substring(resultIndex + 1).split("-");
				for (String knapsackLine : knapsackArray) {
					simulatedAnnealingPrintWriter.println(knapsackLine);
				}
			}

			simulatedAnnealingBufferedReader.close();
			simulatedAnnealingPrintWriter.close();
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new KnapsackSimulatedAnnealing(), args));
	}
}
