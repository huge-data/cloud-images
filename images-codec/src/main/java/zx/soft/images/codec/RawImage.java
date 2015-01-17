package zx.soft.images.codec;

import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

public class RawImage implements Writable, RawComparator<BinaryComparable> {

	private int[] R, G, B, Y, Cr, Cb;
	private int width, height;

	public RawImage(int width, int height, int[] R, int[] G, int[] B, int[] Y, int[] Cb, int[] Cr) {
		this.width = width;
		this.height = height;
		this.R = R;
		this.G = G;
		this.B = B;
		this.Y = Y;
		this.Cr = Cr;
		this.Cb = Cb;
	}

	public RawImage() {
		//
	}

	// Change RawImage to BufferedImage
	public BufferedImage Raw2Buffer() {
		BufferedImage bi = new BufferedImage(this.width, this.height, BufferedImage.TYPE_INT_RGB);
		int RGB_tmp;
		for (int j = 0; j < this.height; j++) {
			for (int i = 0; i < this.width; i++) {
				//				RGB_tmp = new Color(R[j*width+i],G[j*width+i],B[j*width+i]).getRGB();
				RGB_tmp = (R[j * width + i] << 16) | (G[j * width + i] << 8) | (B[j * width + i]);
				bi.setRGB(i, j, RGB_tmp);
			}
		}
		return bi;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		width = input.readInt();
		height = input.readInt();
		byte[] buffer = new byte[width * height * 4];
		input.readFully(buffer);
		R = ByteArraytoIntArray(buffer);
		input.readFully(buffer);
		G = ByteArraytoIntArray(buffer);
		input.readFully(buffer);
		B = ByteArraytoIntArray(buffer);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(width);
		output.writeInt(height);
		output.write(IntArraytoByteArray(R));
		output.write(IntArraytoByteArray(G));
		output.write(IntArraytoByteArray(B));
	}

	@Override
	public int compare(byte[] byte_array1, int start1, int length1, byte[] byte_array2, int start2, int length2) {
		int size1 = length1 * length1;
		int size2 = length2 * length2;

		System.out.println("here in the compare");

		return (size1 - size2);
	}

	@Override
	public int compare(BinaryComparable o1, BinaryComparable o2) {
		byte[] b1 = o1.getBytes();
		byte[] b2 = o2.getBytes();
		int length1 = o1.getLength();
		int length2 = o2.getLength();

		return compare(b1, 0, length1, b2, 0, length2);
	}

	public int getWidth() {
		return this.width;
	}

	public int getHeight() {
		return this.height;
	}

	public int[] getR() {
		return this.R;
	}

	public int[] getG() {
		return this.G;
	}

	public int[] getB() {
		return this.B;
	}

	public int[] getY() {
		return this.Y;
	}

	public int[] getCr() {
		return this.Cr;
	}

	public int[] getCb() {
		return this.Cb;
	}

	public void setY(int[] Y) {
		this.Y = Y;
	}

	public void setR(int[] Y) {
		for (int i = 0; i < Y.length; i++) {
			R[i] = Y[i];
		}
	}

	public void setG(int[] Y) {
		for (int i = 0; i < Y.length; i++) {
			G[i] = Y[i];
		}
	}

	public void setB(int[] Y) {
		for (int i = 0; i < Y.length; i++) {
			B[i] = Y[i];
		}
	}

	public byte[] IntArraytoByteArray(int intArray[]) {
		byte byteArray[] = new byte[intArray.length * 4];
		ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
		IntBuffer intBuf = byteBuf.asIntBuffer();
		intBuf.put(intArray);

		return byteArray;
	}

	public int[] ByteArraytoIntArray(byte byteArray[]) {
		int intArray[] = new int[byteArray.length / 4];
		ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
		IntBuffer intBuf = byteBuf.asIntBuffer();
		intBuf.get(intArray);

		return intArray;
	}

	// YCrCb to RGB
	public void Y2RGB() {
		for (int i = 0; i < R.length; i++) {
			R[i] = (int) (Y[i] + 1.402);
			G[i] = (int) (Y[i] - 0.34414 * (Cb[i] - 128) - 0.71414 * (Cr[i] - 128));
			B[i] = (int) (Y[i] + 1.772 * (Cb[i] - 128));
			if (R[i] > 255)
				R[i] = 255;
			if (G[i] > 255)
				G[i] = 255;
			if (B[i] > 255)
				B[i] = 255;

			if (R[i] < 0)
				R[i] = 0;
			if (G[i] < 0)
				G[i] = 0;
			if (B[i] < 0)
				B[i] = 0;
		}
	}

}
