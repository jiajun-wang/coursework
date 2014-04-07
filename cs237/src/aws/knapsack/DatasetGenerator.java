package aws.knapsack;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * DatasetGenerator is in charge of creating the dataset for testing. All the candidate object data
 * is stored in file "knapsack_dataset.txt". And the input data (keys and thresholds) is stored in
 * file "knapsack_input.txt".
 *
 */
public class DatasetGenerator {

	private static final int MAXIMUM_KEY = 20, COUNT_OF_EACH_KEY = 50000, NUMBER_OF_INPUT_KEYS = 5,
			NUMBER_OF_VALUES = 3;
	private static final double VALUE_UPPERBOUND = 100.0, THRESHOLD_PARAMETER = 0.3;

	public static void main(String[] args) {
		PrintWriter dataPrintWriter = null;

		try {
			dataPrintWriter = new PrintWriter(new FileOutputStream(new File("knapsack_dataset.txt")));

			// totally MAXIMUM_KEY number of different keys
			for (int i = 1; i <= MAXIMUM_KEY; i++) {

				// each key will have COUNT_OF_EACH_KEY number of candidate objects
				for (int j = 0; j < COUNT_OF_EACH_KEY; j++) {
					dataPrintWriter.print(i);

					// each candidate object has NUMBER_OF_VALUES number of values
					for (int k = 1; k <= NUMBER_OF_VALUES; k++) {
						dataPrintWriter.print("\t" + generateRandomValue(VALUE_UPPERBOUND));
					}
					dataPrintWriter.println();
				}
			}

			dataPrintWriter.close();
			dataPrintWriter = new PrintWriter(new FileOutputStream(new File("knapsack_input.txt")));
			Set<Integer> inputKeySet = new HashSet<Integer>();

			// randomly generate NUMBER_OF_INPUT_KEYS number of integer keys
			while (inputKeySet.size() < NUMBER_OF_INPUT_KEYS) {
				inputKeySet.add(new Double(Math.floor(Math.random() * MAXIMUM_KEY + 1)).intValue());
			}

			List<Integer> inputKeyList = new ArrayList<Integer>();
			inputKeyList.addAll(inputKeySet);

			// sort the generated keys
			Collections.sort(inputKeyList);

			// output the generated keys
			for (int i = 0; i < inputKeyList.size(); i++) {
				dataPrintWriter.print(inputKeyList.get(i));

				if (i < inputKeyList.size() - 1) {
					dataPrintWriter.print("\t");
				} else {
					dataPrintWriter.println();
				}
			}

			// output the randomly generated thresholds
			for (int i = 0; i < NUMBER_OF_VALUES; i++) {

				// the upper bound of each threshold is adjusted by THRESHOLD_PARAMETER
				dataPrintWriter
						.print(generateRandomValue(VALUE_UPPERBOUND * NUMBER_OF_INPUT_KEYS * THRESHOLD_PARAMETER));

				if (i < NUMBER_OF_VALUES - 1) {
					dataPrintWriter.print("\t");
				} else {
					dataPrintWriter.println();
				}
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();

		} finally {
			if (dataPrintWriter != null) {
				dataPrintWriter.close();
			}
		}
	}

	/**
	 * generate a string representation of a double value which is created randomly with an upper bound

	 * @param valueUpperbound
	 */
	public static String generateRandomValue(double valueUpperbound) {
		return String.format("%.2f", Math.random() * valueUpperbound);
	}
}
