package image;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

public class MyImageReader {

	public static BufferedImage readImageIntoBufferedImage(String fileName) throws IOException {
		if (fileName == null || !fileName.endsWith(".jpg")) {
			throw new IOException("This is not a jpg file.");
		}

		return ImageIO.read(new File(fileName));
	}

	public static int numChannels(String fileName) throws IOException {
		BufferedImage bufferedImage = readImageIntoBufferedImage(fileName);

		return bufferedImage.getRaster().getNumBands();
	}

	public static int[][][] readImageInto2DArray(String fileName) throws IOException {
		return readImageInto2DArray(readImageIntoBufferedImage(fileName));
	}

	public static int[][][] readImageInto2DArray(BufferedImage bufferedImage) throws IOException {
		WritableRaster writableRaster = bufferedImage.getRaster();

		int height = writableRaster.getHeight();
		int width = writableRaster.getWidth();
		int numBands = writableRaster.getNumBands();
		int[] pixelValues = new int[numBands];
		int imageData[][][] = new int[numBands][height][width];

		for (int x = 0; x < width; x++) {
			for (int y = 0; y < height; y++) {
				pixelValues = writableRaster.getPixel(x, y, pixelValues);

				for (int i = 0; i < numBands; i++) {
					if (pixelValues[i] > Integer.MAX_VALUE) {
						throw new IOException("Pixel value too big for int.");

					} else {
						imageData[i][y][x] = pixelValues[i];
					}
				}
			}
		}

		return imageData;
	}
}
