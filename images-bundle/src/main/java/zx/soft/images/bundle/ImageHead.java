package zx.soft.images.bundle;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ImageHead implements Writable, RawComparator<BinaryComparable> {

	public int imageType;
	public int index;
	public int width;
	public int height;
	public long size;
	public String filename;

	public ImageHead(int index, int imageType, int width, int height, long size, String filename) {
		this.index = index;
		this.imageType = imageType;
		this.width = width;
		this.height = height;
		this.size = size;
		this.filename = filename;
	}

	public ImageHead() {
		//
	}

	public void write_data(DataOutputStream index_stream) throws IOException {
		index_stream.writeInt(index);
		index_stream.writeInt(imageType);
		index_stream.writeInt(width);
		index_stream.writeInt(height);
		index_stream.writeLong(size);
		index_stream.writeUTF(filename);
	}

	public void read(DataInputStream index_stream) throws IOException {
		index = index_stream.readInt();
		imageType = index_stream.readInt();
		width = index_stream.readInt();
		height = index_stream.readInt();
		size = index_stream.readLong();
		filename = index_stream.readUTF();
	}

	@Override
	public void readFields(DataInput index_stream) throws IOException {
		index = index_stream.readInt();
		imageType = index_stream.readInt();
		width = index_stream.readInt();
		height = index_stream.readInt();
		size = index_stream.readLong();
		filename = Text.readString(index_stream);
	}

	@Override
	public void write(DataOutput index_stream) throws IOException {
		index_stream.writeInt(index);
		index_stream.writeInt(imageType);
		index_stream.writeInt(width);
		index_stream.writeInt(height);
		index_stream.writeLong(size);
		index_stream.writeUTF(filename);
	}

	public void write_combine(ByteArrayOutputStream bos) throws IOException {
		ByteBuffer dbuf = ByteBuffer.allocate(24);
		dbuf.putInt(index);
		dbuf.putInt(imageType);
		dbuf.putInt(width);
		dbuf.putInt(height);
		dbuf.putLong(size);
		byte[] byte_tmp = dbuf.array();
		bos.write(byte_tmp);
	}

	@Override
	public int compare(BinaryComparable o1, BinaryComparable o2) {
		return 0;
	}

	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
		return 0;
	}

	public long getSize() {
		return size;
	}

	public int getIndex() {
		return index;
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public String getFilename() {
		return filename;
	}

}
