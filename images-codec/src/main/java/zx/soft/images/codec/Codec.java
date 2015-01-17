package zx.soft.images.codec;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.imageio.ImageIO;

/**
 * Image Type: "bmp", "png", "jpg", "gif", "tiff"
 * 
 * @author wgybzb
 *
 */
public class Codec implements ImageEncoder, ImageDecoder {

	@SuppressWarnings("unused")
	private int RGB, R, G, B, Y, Cr, Cb;
	private int[] RGB_Array, Y_Array, Cb_Array, Cr_Array, R_Array, G_Array, B_Array;
	private int width, height;

	public Codec() {
		System.out.println("This is Codec");
	}

	public byte[] readImageData(InputStream stream) throws IOException {
		System.out.println("readImageData OK");
		if (stream == null)
			throw new IOException("No input image file");
		byte[] buffer = new byte[1024];
		ByteArrayOutputStream output = new ByteArrayOutputStream();

		int numRead = 0;
		while ((numRead = stream.read(buffer)) > -1) {
			output.write(buffer, 0, numRead);
		}

		stream.close();
		output.flush();
		return output.toByteArray();
	}

	public void imageEncoder2(RawImage image, String type, OutputStream os) {
		BufferedImage bi = image.Raw2Buffer();
		try {
			ImageIO.write(bi, type, os);
		} catch (IOException e) {
		}
	}

	public void imageEncoder1(RawImage image, String type, ByteArrayOutputStream os, int rgb, String tmp)
			throws IOException {
		BufferedImage bi = image.Raw2Buffer();
		ImageIO.write(bi, type, new File(tmp + "/1.jpg"));
	}

	// RawImage to OutputStream
	@Override
	public byte[] imageEncoder(RawImage image, String type, OutputStream os, int rgb) throws IOException {
		BufferedImage bi = image.Raw2Buffer();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		BufferedImage nbi = new BufferedImage(bi.getWidth(), bi.getHeight(), rgb == 1 ? BufferedImage.TYPE_INT_RGB
				: BufferedImage.TYPE_BYTE_GRAY);
		Graphics g = nbi.getGraphics();
		g.drawImage(bi, 0, 0, null);
		ImageIO.write(nbi, type, bos);
		g.dispose();
		bos.flush();
		byte[] imageInByte = bos.toByteArray();
		bos.close();

		//		ImageIO.write(bi, type, os);
		return imageInByte;
	}

	// ufferedImage to OutputStream
	@Override
	public void imageEncoder(BufferedImage bi, String type, OutputStream os) throws IOException {
		ImageIO.write(bi, type, os);
	}

	// imageDecoder, the most important one in our project
	@Override
	public RawImage imageDecoder(InputStream is, int type) throws IOException {
		BufferedImage bufferedImage = ImageIO.read(is);
		width = bufferedImage.getWidth();
		height = bufferedImage.getHeight();
		RGB_Array = bufferedImage.getRGB(0, 0, width, height, null, 0, width);

		//Init for Array
		R_Array = new int[height * width];
		G_Array = new int[height * width];
		B_Array = new int[height * width];
		Y_Array = new int[height * width];
		Cb_Array = new int[height * width];
		Cr_Array = new int[height * width];

		for (int j = 0; j < height; j++) {
			for (int i = 0; i < width; i++) {
				//For RGB
				R = RGB_Array[j * width + i] >> 16 & 0xFF;
				G = RGB_Array[j * width + i] >> 8 & 0xFF;
				B = RGB_Array[j * width + i] >> 0 & 0xFF;
				R_Array[j * width + i] = R;
				G_Array[j * width + i] = G;
				B_Array[j * width + i] = B;

				//For YCbCr
				Y = (int) (0.299 * R + 0.587 * G + 0.114 * B);
				Cb = (int) (-0.16874 * R - 0.33126 * G + 0.50000 * B);
				Cr = (int) (0.50000 * R - 0.41869 * G - 0.08131 * B);
				Y = Y > 255 ? 255 : Y;
				Cb = Cb > 255 ? 255 : Cb;
				Cr = Cr > 255 ? 255 : Cr;
				Y = Y < 0 ? 0 : Y;
				Cr = Cr < 0 ? 0 : Y;
				Cb = Cb < 0 ? 0 : Y;
				Y_Array[j * width + i] = Y;
				Cb_Array[j * width + i] = Cb;
				Cr_Array[j * width + i] = Cr;
			}
		}
		return new RawImage(width, height, R_Array, G_Array, B_Array, Y_Array, Cb_Array, Cr_Array);
	}

	/**
	 * 主函数
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		Codec codec = new Codec();
		if (args[0].equals("decode")) {
			try {
				InputStream is = new FileInputStream(args[1]);
				RawImage ri = codec.imageDecoder(is, 1);
			} catch (Exception e) {
				//
			}
			for (int i = 0; i < codec.R_Array.length; i++) {
				System.out.println(codec.R_Array[i]);
			}
		} else if (args[0].equals("encode")) {
			try {
				File fp = new File(args[1]);
				BufferedImage bi = ImageIO.read(fp);
				FileOutputStream os = new FileOutputStream(args[3]);
				codec.imageEncoder(bi, args[2], os);
				os.flush();
				os.close();
			} catch (Exception e) {
			}
		}
	}

}
