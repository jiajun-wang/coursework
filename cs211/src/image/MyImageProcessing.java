package image;

import java.awt.image.BufferedImage;
import java.io.IOException;

public class MyImageProcessing {

	public static void writeOriginalImage(String inputImageFileName, String outputImageFileName) throws IOException {
		BufferedImage inputBufferedImage = MyImageReader.readImageIntoBufferedImage(inputImageFileName);
		int[][][] inputImageData = MyImageReader.readImageInto2DArray(inputBufferedImage);

		MyImageWriter.writeImage(outputImageFileName + "_origin.jpg", inputImageData, inputBufferedImage,
									inputBufferedImage.getWidth(), inputBufferedImage.getHeight());
	}

	public static void generateGaussianPyramid(String inputImageFileName, String outputImageFileName)
			throws IOException {

		BufferedImage inputBufferedImage = MyImageReader.readImageIntoBufferedImage(inputImageFileName);
		int[][][] initialImageData = MyImageReader.readImageInto2DArray(inputBufferedImage);
		int[][][] inputImageData = initialImageData;
		int[][][] outputImageData = null;

		int inputSize = initialImageData[0].length;
		int outputSize = 0;
		double[][] filter = { {0.25, 0.25}, {0.25, 0.25}};

		for (; inputSize >= 2; inputSize = outputSize, inputImageData = outputImageData) {
			outputSize = inputSize / 2;
			String extensionName = "_gp1_" + outputSize + ".jpg";

			outputImageData = convolveAndScale(inputImageData, filter, inputSize, inputSize, outputSize, outputSize);

			MyImageWriter.writeImage(outputImageFileName + extensionName, outputImageData, inputBufferedImage,
										outputSize, outputSize);
		}

		inputImageData = initialImageData;
		outputSize = initialImageData[0].length;
		int filterSize = 2;
		double filterValue = 0.25;
		double sumValue = 0.0;
		int filteredValue = 0;

		for (; filterSize <= initialImageData[0].length; filterSize *= 2, inputImageData = outputImageData) {
			String extensionName = "_gp2_" + filterSize + ".jpg";

			outputImageData = new int[inputImageData.length][outputSize][outputSize];

			for (int i = 0; i < inputImageData.length; i++) {
				for (int j = 0; j < inputImageData[i].length; j += filterSize) {
					for (int k = 0; k < inputImageData[i][j].length; k += filterSize) {
						sumValue = 0.0;

						for (int m = 0; m < filterSize; m += filterSize / 2) {
							for (int n = 0; n < filterSize; n += filterSize / 2) {
								sumValue += inputImageData[i][j + m][k + n] * filterValue;
							}
						}

						filteredValue = Math.max(Math.min((int) sumValue, 255), 0);;

						for (int m = 0; m < filterSize; m++) {
							for (int n = 0; n < filterSize; n++) {
								outputImageData[i][j + m][k + n] = filteredValue;
							}
						}
					}
				}
			}

			MyImageWriter.writeImage(outputImageFileName + extensionName, outputImageData, inputBufferedImage,
										outputSize, outputSize);
		}
	}

	public static void generateLaplacianPyramid(String inputImageFileName, String outputImageFileName)
			throws IOException {

		BufferedImage firstBufferedImage = MyImageReader.readImageIntoBufferedImage(inputImageFileName);
		int[][][] firstImageData = MyImageReader.readImageInto2DArray(firstBufferedImage);
		BufferedImage secondBufferedImage = null;
		int[][][] secondImageData = null;
		int[][][] outputImageData = null;
		int outputSize = firstImageData[0].length;

		for (int filterSize = 2; filterSize <= 256; filterSize *= 2) {
			if (filterSize != 2) {
				firstBufferedImage = secondBufferedImage;
				firstImageData = secondImageData;
			}

			secondBufferedImage = MyImageReader.readImageIntoBufferedImage(outputImageFileName + "_gp2_" + filterSize
					+ ".jpg");
			secondImageData = MyImageReader.readImageInto2DArray(secondBufferedImage);

			outputImageData = new int[firstImageData.length][outputSize][outputSize];

			for (int i = 0; i < firstImageData.length; i++) {
				for (int j = 0; j < firstImageData[i].length; j++) {
					for (int k = 0; k < firstImageData[i][j].length; k++) {

						outputImageData[i][j][k] = firstImageData[i][j][k] - secondImageData[i][j][k];
						outputImageData[i][j][k] = Math.max(Math.min(outputImageData[i][j][k], 255), 0);
					}
				}
			}

			MyImageWriter.writeImage(outputImageFileName + "_lp_" + filterSize + ".jpg", outputImageData,
										firstBufferedImage, outputSize, outputSize);
		}
	}

	public static void detectMultiScaleEdge(String inputImageFileName, String outputImageFileName, double edgeThreshold)
			throws IOException {

		for (int i = 1; i <= 128; i *= 2) {
			String extensionName = "_med_gp1_" + i;

			detectEdge(inputImageFileName + extensionName + ".jpg", outputImageFileName, edgeThreshold, extensionName);
		}

		for (int i = 2; i <= 256; i *= 2) {
			String extensionName = "_med_gp2_" + i;

			detectEdge(inputImageFileName + extensionName + ".jpg", outputImageFileName, edgeThreshold, extensionName);
		}
	}

	public static void detectEdge(String inputImageFileName, String outputImageFileName, int thresholdStep)
			throws IOException {

		for (int i = 0; i < 8; i++) {
			int edgeThreshold = i * thresholdStep;

			detectEdge(inputImageFileName, outputImageFileName, edgeThreshold, "_oed_" + edgeThreshold);
		}
	}

	public static void detectEdge(String inputImageFileName, String outputImageFileName, double edgeThreshold,
			String extensionName) throws IOException {

		BufferedImage inputBufferedImage = MyImageReader.readImageIntoBufferedImage(inputImageFileName);
		int[][][] inputImageData = MyImageReader.readImageInto2DArray(inputBufferedImage);

		int inputSize = inputImageData[0].length;
		double[][] filter = { {-0.125, -0.125, -0.125}, {-0.125, 1, -0.125}, {-0.125, -0.125, -0.125}};

		double[][][] secondOrderDerivativeImageData = convolve(inputImageData, filter, inputSize, inputSize, inputSize,
																inputSize);;

		int[][][] segmentedImageData = new int[inputImageData.length][inputSize][inputSize];

		for (int i = 0; i < inputImageData.length; i++) {
			for (int j = 0; j < inputImageData[i].length; j++) {
				for (int k = 0; k < inputImageData[i][j].length; k++) {
					segmentedImageData[i][j][k] = secondOrderDerivativeImageData[i][j][k] > 0.0 ? 1 : 0;
				}
			}
		}

		boolean[][][] zeroCrossingImageData = new boolean[inputImageData.length][inputSize][inputSize];

		for (int i = 0; i < inputImageData.length; i++) {
			for (int j = 0; j < inputImageData[i].length; j++) {
				for (int k = 0; k < inputImageData[i][j].length; k++) {

					zeroCrossingImageData[i][j][k] = false;
					int selfValue = segmentedImageData[i][j][k];

					if (j - 1 >= 0) {
						if (k - 1 >= 0) {
							if (segmentedImageData[i][j - 1][k - 1] != selfValue) {
								zeroCrossingImageData[i][j][k] = true;
								continue;
							}
						}

						if (segmentedImageData[i][j - 1][k] != selfValue) {
							zeroCrossingImageData[i][j][k] = true;
							continue;
						}

						if (k + 1 < inputImageData[i][j].length) {
							if (segmentedImageData[i][j - 1][k + 1] != selfValue) {
								zeroCrossingImageData[i][j][k] = true;
								continue;
							}

						}
					}

					if (k - 1 >= 0) {
						if (segmentedImageData[i][j][k - 1] != selfValue) {
							zeroCrossingImageData[i][j][k] = true;
							continue;
						}
					}
					if (k + 1 < inputImageData[i][j].length) {
						if (segmentedImageData[i][j][k + 1] != selfValue) {
							zeroCrossingImageData[i][j][k] = true;
							continue;
						}
					}

					if (j + 1 < inputImageData[i].length) {
						if (k - 1 >= 0) {
							if (segmentedImageData[i][j + 1][k - 1] != selfValue) {
								zeroCrossingImageData[i][j][k] = true;
								continue;
							}
						}

						if (segmentedImageData[i][j + 1][k] != selfValue) {
							zeroCrossingImageData[i][j][k] = true;
							continue;
						}

						if (k + 1 < inputImageData[i][j].length) {
							if (segmentedImageData[i][j + 1][k + 1] != selfValue) {
								zeroCrossingImageData[i][j][k] = true;
								continue;
							}
						}
					}
				}
			}
		}

		int[][][] outputImageData = new int[inputImageData.length][inputSize][inputSize];
		double localCount = 0.0;
		double localSum = 0.0;
		double localMean = 0.0;
		double localVariance = 0.0;

		for (int i = 0; i < inputImageData.length; i++) {
			for (int j = 0; j < inputImageData[i].length; j++) {
				for (int k = 0; k < inputImageData[i][j].length; k++) {

					if (zeroCrossingImageData[i][j][k]) {
						localCount = 0.0;
						localSum = 0.0;

						if (j - 1 >= 0) {
							if (k - 1 >= 0) {
								localCount++;
								localSum += secondOrderDerivativeImageData[i][j - 1][k - 1];
							}

							localCount++;
							localSum += secondOrderDerivativeImageData[i][j - 1][k];

							if (k + 1 < inputImageData[i][j].length) {
								localCount++;
								localSum += secondOrderDerivativeImageData[i][j - 1][k + 1];
							}
						}

						if (k - 1 >= 0) {
							localCount++;
							localSum += secondOrderDerivativeImageData[i][j][k - 1];
						}

						localCount++;
						localSum += secondOrderDerivativeImageData[i][j][k];

						if (k + 1 < inputImageData[i][j].length) {
							localCount++;
							localSum += secondOrderDerivativeImageData[i][j][k + 1];
						}

						if (j + 1 < inputImageData[i].length) {
							if (k - 1 >= 0) {
								localCount++;
								localSum += secondOrderDerivativeImageData[i][j + 1][k - 1];
							}

							localCount++;
							localSum += secondOrderDerivativeImageData[i][j + 1][k];

							if (k + 1 < inputImageData[i][j].length) {
								localCount++;
								localSum += secondOrderDerivativeImageData[i][j + 1][k + 1];
							}
						}

						localMean = localSum / localCount;
						localVariance = 0.0;

						if (j - 1 >= 0) {
							if (k - 1 >= 0) {
								localVariance += Math.pow(secondOrderDerivativeImageData[i][j - 1][k - 1] - localMean,
															2.0);
							}

							localVariance += Math.pow(secondOrderDerivativeImageData[i][j - 1][k] - localMean, 2.0);

							if (k + 1 < inputImageData[i][j].length) {
								localVariance += Math.pow(secondOrderDerivativeImageData[i][j - 1][k + 1] - localMean,
															2.0);
							}
						}

						if (k - 1 >= 0) {
							localVariance += Math.pow(secondOrderDerivativeImageData[i][j][k - 1] - localMean, 2.0);
						}

						localVariance += Math.pow(secondOrderDerivativeImageData[i][j][k] - localMean, 2.0);

						if (k + 1 < inputImageData[i][j].length) {
							localVariance += Math.pow(secondOrderDerivativeImageData[i][j][k + 1] - localMean, 2.0);
						}

						if (j + 1 < inputImageData[i].length) {
							if (k - 1 >= 0) {
								localVariance += Math.pow(secondOrderDerivativeImageData[i][j + 1][k - 1] - localMean,
															2.0);
							}

							localVariance += Math.pow(secondOrderDerivativeImageData[i][j + 1][k] - localMean, 2.0);

							if (k + 1 < inputImageData[i][j].length) {
								localVariance += Math.pow(secondOrderDerivativeImageData[i][j + 1][k + 1] - localMean,
															2.0);
							}
						}

						localVariance /= localCount;

						outputImageData[i][j][k] = localVariance > edgeThreshold ? 0 : 255;

					} else {
						outputImageData[i][j][k] = 255;
					}
				}
			}
		}

		MyImageWriter.writeImage(outputImageFileName + extensionName + ".jpg", outputImageData, inputBufferedImage,
									inputSize, inputSize);
	}

	public static int[][][] convolveAndScale(int[][][] inputImageData, double[][] filter, int inputWidth,
			int inputHeight, int outputWidth, int outputHeight) {

		int[][][] outputImageData = new int[inputImageData.length][outputHeight][outputWidth];

		for (int i = 0; i < inputImageData.length; i++) {
			for (int j = 0; j < inputImageData[i].length / 2; j++) {
				for (int k = 0; k < inputImageData[i][j].length / 2; k++) {

					outputImageData[i][j][k] = (int) (inputImageData[i][j * 2][k * 2] * filter[0][0]
							+ inputImageData[i][j * 2][k * 2 + 1] * filter[0][1] + inputImageData[i][j * 2 + 1][k * 2]
							* filter[1][0] + inputImageData[i][j * 2 + 1][k * 2 + 1] * filter[1][1]);

					outputImageData[i][j][k] = Math.max(Math.min(outputImageData[i][j][k], 255), 0);
				}
			}
		}

		return outputImageData;
	}

	public static double[][][] convolve(int[][][] inputImageData, double[][] filter, int inputWidth, int inputHeight,
			int outputWidth, int outputHeight) {

		double[][][] outputImageRawData = new double[inputImageData.length][outputHeight][outputWidth];

		for (int i = 0; i < inputImageData.length; i++) {
			for (int j = 0; j < inputImageData[i].length; j++) {
				for (int k = 0; k < inputImageData[i][j].length; k++) {

					if (j - 1 >= 0) {
						if (k - 1 >= 0) {
							outputImageRawData[i][j][k] += inputImageData[i][j - 1][k - 1] * filter[0][0];
						}

						outputImageRawData[i][j][k] += inputImageData[i][j - 1][k] * filter[0][1];

						if (k + 1 < inputImageData[i][j].length) {
							outputImageRawData[i][j][k] += inputImageData[i][j - 1][k + 1] * filter[0][2];
						}
					}

					if (k - 1 >= 0) {
						outputImageRawData[i][j][k] += inputImageData[i][j][k - 1] * filter[1][0];
					}

					outputImageRawData[i][j][k] += inputImageData[i][j][k] * filter[1][1];

					if (k + 1 < inputImageData[i][j].length) {
						outputImageRawData[i][j][k] += inputImageData[i][j][k + 1] * filter[1][2];
					}

					if (j + 1 < inputImageData[i].length) {
						if (k - 1 >= 0) {
							outputImageRawData[i][j][k] += inputImageData[i][j + 1][k - 1] * filter[2][0];
						}

						outputImageRawData[i][j][k] += inputImageData[i][j + 1][k] * filter[2][1];

						if (k + 1 < inputImageData[i][j].length) {
							outputImageRawData[i][j][k] += inputImageData[i][j + 1][k + 1] * filter[2][2];
						}
					}
				}
			}
		}

		return outputImageRawData;
	}

	public static void main(String[] args) throws IOException {
		String inputImageFileName1 = "input/kitty.jpg";
		String outputImageFileName1 = "output/kitty/mip_kitty";

		String inputImageFileName2 = "input/CARTOON.jpg";
		String outputImageFileName2 = "output/cartoon/mip_CARTOON";

		String inputImageFileName3 = "input/flowergray.jpg";
		String outputImageFileName3 = "output/flowergray/mip_flowergray";

		String inputImageFileName4 = "input/polarcities.jpg";
		String outputImageFileName4 = "output/polarcities/mip_polarcities";

		String inputImageFileName5 = "input/text.jpg";
		String outputImageFileName5 = "output/text/mip_text";

		String inputImageFileName = inputImageFileName4;
		String outputImageFileName = outputImageFileName4;
		double edgeThreshold = 50.0;
		int thresholdStep = 300;

		writeOriginalImage(inputImageFileName, outputImageFileName);

		generateGaussianPyramid(inputImageFileName, outputImageFileName);

		generateLaplacianPyramid(inputImageFileName, outputImageFileName);

		detectMultiScaleEdge(outputImageFileName, outputImageFileName, edgeThreshold);

		detectEdge(inputImageFileName, outputImageFileName, thresholdStep);
	}
}
