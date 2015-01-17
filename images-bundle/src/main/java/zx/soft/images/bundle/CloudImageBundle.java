package zx.soft.images.bundle;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CloudImageBundle extends AbstractImageBundle {

	private DataInputStream index_read_stream = null;
	private DataInputStream data_read_stream = null;
	private DataOutputStream index_write_stream = null;
	private DataOutputStream data_write_stream = null;
	private Path bundle_path;
	private final Configuration conf;
	private int index;

	// Variable for speed up
	private String filename;

	public CloudImageBundle(Configuration conf) {
		this.conf = conf;
	}

	public CloudImageBundle(Path bundle_path, Configuration conf) {
		this.bundle_path = bundle_path;
		this.conf = conf;
	}

	public CloudImageBundle(Path bundle_path, int index, Configuration conf) {
		this.bundle_path = bundle_path;
		this.conf = conf;
		this.index = index;
	}

	@Override
	public void open(int mode) throws IOException {
		if (mode == 0) { // Read
			open_read();
		} else { // Write
			open_write();
		}
	}

	public void addImage(String image_path, int index) throws IOException {
		this.index = index;
		addImage(image_path);
	}

	@Override
	public void addImage(String image_path) throws IOException {
		File image_file = new File(image_path);
		byte data[] = readImageData(new FileInputStream(image_file));
		System.out.println("Create image File class");
		long offset = ((FSDataOutputStream) data_write_stream).getPos();
		System.out.println("Offset is " + offset);
		System.out.println("Filename is" + image_file.getName());
		// Get Image's feature and index
		BufferedImage bi = ImageIO.read(image_file);
		int width = bi.getWidth();
		int height = bi.getHeight();
		// Get information for image index
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss");
		Date date = new Date();
		String create_time = dateformat.format(date);
		String modify_time = create_time;
		UUID uuid = UUID.randomUUID();
		String username = System.getProperty("user.name");
		// Write UUID to index file
		ImageIndex ii = new ImageIndex(uuid.toString(), index, 1, 1, width, height, offset, image_file.length(),
				username, image_file.getName(), create_time, modify_time, "memo");
		ii.write(index_write_stream);

		// Write ImageHead to data file
		ImageHead ih = new ImageHead(index, 1, width, height, image_file.length(), image_file.getName());
		ih.write(data_write_stream);

		// Write ImageFile to data file
		data_write_stream.write(data);
	}

	/* Delete the image file in bundle with the specific name
	   Parameter: image_path - cloudimagebundle (cib) path
				  file_name  - the filename the user wants to delete
	   Return: n for successfully delete files' number; 0 for not found;
	*/
	public int delImage(Path image_path, String file_name, int index) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		String tmp_index = "/tmp-index";
		DataOutputStream new_index_write_stream = fs.create(new Path(tmp_index));
		index_read_stream = new DataInputStream(FileSystem.get(conf).open(image_path));
		int magic_code = index_read_stream.readInt();
		if (magic_code != 0x00434942) {
			return -1;
		}

		ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
		new_index_write_stream.writeInt(0x00434942);
		int change = 0;
		for (int i = 0; i < index; i++) {
			ii.read(index_read_stream);
			if (ii.filename.equals(file_name)) {
				ii.valid = 0;
				change++;
			}
			ii.write(new_index_write_stream);
		}

		index_read_stream.close();
		new_index_write_stream.close();

		return change;
	}

	/*
	//Need to be updated
	public void removeImage(Path image_path) {
		int offset_tmp;
		File image_file = new File(image_path);
		ImageIndex ii = new ImageIndex(1, 1, 1, 0, 0, "username", image_file.getName(), null, null, "memo");

		//Start at the beginning of file
		index_write_stream.seek(0);
		while(ii.read(index_write_stream)) {
			if (ii.filename.equals(filename)) {
				ii.valid = 0;
				break;
			}
		}
	}
	*/

	public int update(Path old_index, Path old_data, int index) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		long new_offset = 0;
		int new_index = 0;
		int sum_delete = 0;
		String tmp_index = "/tmp-index";
		String tmp_data = "/tmp-data";
		DataOutputStream new_index_write_stream = fs.create(new Path(tmp_index));
		FSDataOutputStream new_data_write_stream = fs.create(new Path(tmp_data));

		index_read_stream = new DataInputStream(FileSystem.get(conf).open(old_index));
		data_read_stream = new DataInputStream(FileSystem.get(conf).open(old_data));
		int magic_code = index_read_stream.readInt();
		if (magic_code != 0x00434942) {
			return -1;
		}

		ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
		ImageHead ih = new ImageHead(ii.getIndex(), ii.getType(), ii.getWidth(), ii.getHeight(), ii.getLength(),
				ii.getFilename());

		new_index_write_stream.writeInt(0x00434942);

		for (int i = 0; i < index; i++) {
			ii.read(index_read_stream);
			ih.read(data_read_stream);
			if (ii.valid == 1) {
				ii.setIndex(new_index);
				ii.setOffset(new_offset);

				SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss");
				Date date = new Date();
				String modify_time = dateformat.format(date);
				ii.setModify_time(modify_time);

				ii.write(new_index_write_stream);

				ih.index = new_index;
				ih.filename = ii.getFilename();

				ih.write(new_data_write_stream);

				byte[] data_write_byte = new byte[(int) (ii.getLength())];
				data_read_stream.readFully(data_write_byte);
				new_data_write_stream.write(data_write_byte);

				new_offset = new_data_write_stream.getPos();
				new_index++;
			} else {
				data_read_stream.skip(ii.getLength());
				sum_delete++;
			}
		}

		index_read_stream.close();
		data_read_stream.close();
		new_index_write_stream.close();
		new_data_write_stream.close();

		return sum_delete;
	}

	@Override
	public int open_read() throws IOException {
		int magic_code;
		if (bundle_path == null) {
			throw new IOException("There is something wrong in open bundle file.\n");
		}
		index_read_stream = new DataInputStream(FileSystem.get(conf).open(bundle_path));
		data_read_stream = new DataInputStream(FileSystem.get(conf).open(new Path(bundle_path.toString() + "_dat")));
		// Read Bundel Head method
		magic_code = index_read_stream.readInt();
		if (magic_code != 0x00434942) {
			System.out.println("This file is not a Bundle file.\n");
			return -1;
		}
		return 0;
	}

	@Override
	public int open_write() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		System.out.println("open write init is right.");
		if (!fs.exists(bundle_path)) {
			System.out.println("Create a new bundle.");
			index_write_stream = fs.create(bundle_path);
			data_write_stream = fs.create(new Path(bundle_path.toString() + "_dat"));
			System.out.println("Create a new Bundle File\n");
			index_write_stream.writeInt(0x00434942); //Write a magic code
		} else {
			System.out.println("Append a existing bundle.");
			index_write_stream = fs.append(bundle_path);
			data_write_stream = fs.append(new Path(bundle_path.toString() + "_dat"));
		}
		return 0;
	}

	@Override
	public void open_close() throws IOException {
		if (index_read_stream != null)
			index_read_stream.close();
		if (data_read_stream != null)
			data_read_stream.close();
		if (index_write_stream != null)
			index_write_stream.close();
		if (data_write_stream != null)
			data_write_stream.close();
	}

	@Override
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

	public void listImage() throws IOException {
		int first = 1;
		int sum = 0;
		ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
		open(0);
		for (int i = 0; i < index; i++) {
			ii.read(index_read_stream);
			if (first == 1) {
				ii.print_row();
				first = 0;
			}
			if (ii.valid == 1) {
				ii.print();
				sum++;
			}
		}
		open_close();
		System.out.println("The number of files in this bundle is " + sum + ".");
	}

	public InputStream getIndexReadStream() {
		return index_read_stream;
	}

	public boolean seek(long i, long j) {
		try {
			data_read_stream.skip(j - i);
			return true;
		} catch (IOException e) {
			System.out.println(e.toString());
			return false;
		}
	}

	public DataInputStream getInputStream() {
		return data_read_stream;
	}

	public Path getPath() {
		return bundle_path;
	}

	public String getFilename() {
		return filename;
	}

	public byte[] getImage(int i) throws IOException {
		long offset_tmp = 0;
		int size_tmp = 0;
		byte[] tmp;
		int found = 0;
		ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
		ImageHead ih = new ImageHead(ii.getIndex(), ii.getType(), ii.getWidth(), ii.getHeight(), ii.getLength(),
				ii.getFilename());

		open(0);
		for (int j = 0; j < index; j++) {
			ii.read(index_read_stream);
			if (ii.valid == 0)
				continue;
			if (j == i) {
				offset_tmp = ii.offset;
				size_tmp = (int) ii.length;
				this.filename = ii.filename;
				found = 1;
				break;
			}
		}

		if (found == 1) {
			long skip_length = offset_tmp;
			while (skip_length > 0) {
				long skipped = data_read_stream.skip(skip_length);
				if (skipped == 0)
					break;
				skip_length -= skipped;
			}
			// Read filename in ImageHead
			// Modified in 07/16
			ih.read(data_read_stream);
			//	data_read_stream.readUTF();			

			tmp = new byte[size_tmp];

			int readCount = 0;
			int count = size_tmp;
			while (readCount < count) {
				readCount += data_read_stream.read(tmp, readCount, count - readCount);
			}

			open_close();
		} else {
			open_close();
			return null;
		}
		return tmp;
	}

	@Override
	public byte[] getImage(String filename) throws IOException {
		long offset_tmp = 0;
		int size_tmp = 0;
		byte[] tmp;
		int found = 0;
		ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
		open(0);
		while (ii.read(index_read_stream)) {
			if (ii.filename.equals(filename)) {
				offset_tmp = ii.offset;
				size_tmp = (int) ii.length;
				found = 1;
				break;
			}
		}

		if (found == 1) {
			data_read_stream.skip(offset_tmp + 4 * 6);
			tmp = new byte[size_tmp];
			data_read_stream.read(tmp, 0, size_tmp);
			open_close();
		} else {
			open_close();
			return null;
		}

		return tmp;
	}

	public void combine(String input_dir, String output_dir, String index_dir) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path new_path = new Path(input_dir);
		Path old_index = new Path(index_dir + "/result");
		Path new_index = new Path(output_dir + "/result");
		FileStatus[] status = fs.listStatus(new_path);
		ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");

		index_write_stream = fs.create(new_index);
		data_write_stream = fs.create(new Path(new_index.toString() + "_dat"));
		index_write_stream.writeInt(0x00434942); //Write a magic code

		ByteArrayOutputStream output = new ByteArrayOutputStream();

		for (int i = 0; i < status.length - 2; i++) {
			if (i % 10000 == 0)
				System.out.println("Processing the " + i + " image.");
			int key = Integer.parseInt(status[i].getPath().getName());
			// Open old index
			index_read_stream = new DataInputStream(fs.open(old_index));
			// Read the magic code
			index_read_stream.readInt();
			// System.out.println(key);
			while (ii.read(index_read_stream)) {
				// If find index successfully
				if (ii.getIndex() == key) {
					break;
				}
			}

			// Continue to get information

			long offset = ((FSDataOutputStream) data_write_stream).getPos();

			long length = status[i].getLen();
			SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss");
			Date date = new Date();
			String create_time = dateformat.format(date);
			String modify_time = create_time;
			UUID uuid = UUID.randomUUID();
			// Set the index information			

			ii.setIID(uuid.toString());
			ii.setIndex(i);
			ii.setCreate_time(create_time);
			ii.setModify_time(modify_time);
			ii.setOffset(offset);
			ii.setLength(length);
			ii.setFilename(ii.getFilename().substring(0, ii.getFilename().lastIndexOf(".")) + ".jpg");
			ii.write(index_write_stream);

			// Write Bundle Data Part					
			ImageHead ih = new ImageHead(ii.getIndex(), ii.getType(), ii.getWidth(), ii.getHeight(), ii.getLength(),
					ii.getFilename());
			ih.write_combine(output);
			byte[] buffer = new byte[(int) length];
			//	int numRead = 0;
			DataInputStream stream = fs.open(status[i].getPath());
			stream.readFully(buffer);
			output.write(buffer);
			buffer = null;
			stream.close();
			//System.out.println(output.size());
			if (output.size() > 1024 * 1024 * 128) {
				byte[] buf = output.toByteArray();
				((FSDataOutputStream) data_write_stream).write(buf);
				buf = null;
				output.reset();
			}
			index_read_stream.close();
		}

		// Stop here
		index_write_stream.close();
		data_write_stream.close();

		Path path_num = new Path(new_index.toString() + "_num");
		DataOutputStream dos = fs.create(path_num);
		dos.writeInt(status.length - 2);
		dos.close();

		//System.out.println("Successfully");
	}

}
