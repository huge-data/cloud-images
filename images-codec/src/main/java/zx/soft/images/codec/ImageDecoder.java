package zx.soft.images.codec;

import java.io.IOException;
import java.io.InputStream;

public interface ImageDecoder {

	public RawImage imageDecoder(InputStream is, int type) throws IOException;

}
