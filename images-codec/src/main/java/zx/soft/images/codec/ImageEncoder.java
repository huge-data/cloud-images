package zx.soft.images.codec;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

public interface ImageEncoder {

	public byte[] imageEncoder(RawImage image, String type, OutputStream os, int rgb) throws IOException;

	public void imageEncoder(BufferedImage bi, String type, OutputStream os) throws IOException;

}
