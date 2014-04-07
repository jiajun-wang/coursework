package image;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

public class MyImageWriter {

	public static boolean writeImage(String outputFileName, int[][][] imageData, BufferedImage inputBufferedImage,
			int outputWidth, int outputHeight) throws IOException {

		WritableRaster outputWritableRaster = inputBufferedImage.getRaster()
				.createCompatibleWritableRaster(outputWidth, outputHeight);
		int[] pixelData = new int[outputWritableRaster.getNumBands()];

		for (int y = 0; y < outputWritableRaster.getHeight(); y++) {
			for (int x = 0; x < outputWritableRaster.getWidth(); x++) {
				for (int i = 0; i < pixelData.length; i++) {
					pixelData[i] = imageData[i][y][x];
				}

				outputWritableRaster.setPixel(x, y, pixelData);
			}
		}

		BufferedImage outputBufferedImage = new BufferedImage(outputWidth, outputHeight, inputBufferedImage.getType());
		outputBufferedImage.setData(outputWritableRaster);

		boolean result = ImageIO.write(outputBufferedImage, "jpg", new File(outputFileName));

		if (!result) {
			System.out.println("Could not find image format for output image.");
		}

		return result;
	}
}
