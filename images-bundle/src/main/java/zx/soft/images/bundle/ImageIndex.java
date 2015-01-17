package zx.soft.images.bundle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ImageIndex {

	protected String IID;
	protected int type;
	protected int index;
	protected int valid;
	protected long offset;
	protected long length;
	protected int width;
	protected int height;
	protected String username;
	protected String filename;
	protected String create_time;
	protected String modify_time;
	protected String description;

	public ImageIndex(String IID, int index, int type, int valid, int width, int height, long offset, long length,
			String username, String filename, String create_time, String modify_time, String description) {
		this.IID = IID;
		this.index = index;
		this.type = type;
		this.valid = valid;
		this.width = width;
		this.height = height;
		this.offset = offset;
		this.length = length;
		this.username = username;
		this.filename = filename;
		this.create_time = create_time;
		this.modify_time = modify_time;
		this.description = description;
	}

	public void write(DataOutputStream index_stream) throws IOException {
		index_stream.writeUTF(IID);
		index_stream.writeInt(index);
		index_stream.writeInt(type);
		index_stream.writeInt(valid);
		index_stream.writeInt(width);
		index_stream.writeInt(height);
		index_stream.writeLong(offset);
		index_stream.writeLong(length);
		index_stream.writeUTF(username);
		index_stream.writeUTF(filename);
		index_stream.writeUTF(create_time);
		index_stream.writeUTF(modify_time);
		index_stream.writeUTF(description);
	}

	public boolean read(DataInputStream index_stream) {
		try {
			IID = index_stream.readUTF();
			index = index_stream.readInt();
			type = index_stream.readInt();
			valid = index_stream.readInt();
			width = index_stream.readInt();
			height = index_stream.readInt();
			offset = index_stream.readLong();
			length = index_stream.readLong();
			username = index_stream.readUTF();
			filename = index_stream.readUTF();
			create_time = index_stream.readUTF();
			modify_time = index_stream.readUTF();
			description = index_stream.readUTF();
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
		return true;
	}

	public void print_row() {
		System.out.print("index");
		System.out.print("type ");
		System.out.print("valid ");
		System.out.print("offset ");
		System.out.print("width ");
		System.out.print("height ");
		System.out.print("length ");
		System.out.print("username ");
		System.out.print("filename ");
		System.out.print("create_time ");
		System.out.print("modify_time ");
		System.out.println("description");
	}

	public void print() {
		System.out.print(index + " ");
		System.out.print(type + " ");
		System.out.print(valid + " ");
		System.out.print(width + " ");
		System.out.print(height + " ");
		System.out.print(offset + " ");
		System.out.print(length + " ");
		System.out.print(username + " ");
		System.out.print(filename + " ");
		System.out.print(create_time + " ");
		System.out.print(modify_time + " ");
		System.out.println(description);
	}

	public String getIID() {
		return IID;
	}

	public int getIndex() {
		return index;
	}

	public int getType() {
		return type;
	}

	public int getValid() {
		return valid;
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public long getOffset() {
		return offset;
	}

	public long getLength() {
		return length;
	}

	public String getUsername() {
		return username;
	}

	public String getFilename() {
		return filename;
	}

	public String getCreate_time() {
		return create_time;
	}

	public String getModify_time() {
		return modify_time;
	}

	public String getDescription() {
		return description;
	}

	public void setIID(String IID) {
		this.IID = IID;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public void setType(int type) {
		this.type = type;
	}

	public void setValid(int valid) {
		this.valid = valid;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}

	public void setModify_time(String modify_time) {
		this.modify_time = modify_time;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public void setHeight(int height) {
		this.height = height;
	}

}
