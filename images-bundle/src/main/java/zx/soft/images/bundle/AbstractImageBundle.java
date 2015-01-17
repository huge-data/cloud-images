package zx.soft.images.bundle;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

@SuppressWarnings("unused")
public abstract class AbstractImageBundle {

	private String bundle_name; //Bundle's name
	private Path bundle_path; //Bundle's path
	private Date create_date; //Bundle's create date
	private Date modify_date; //Bundle's modify date

	private int open_mode; //Use Bundle for open or write
	private int only_append; //To see whether can write a new 

	protected Configuration conf; //Configuration

	public AbstractImageBundle() {
		//
	}

	public abstract void open(int mode) throws IOException; //Open for ImageBundle

	public abstract void addImage(String image_path) throws IOException; //Add Image into Bundle File

	public abstract void open_close() throws IOException; //Close all open stream

	public abstract int open_read() throws IOException; //Open for read

	public abstract int open_write() throws IOException; //Open for write

	public abstract byte[] readImageData(InputStream stream) throws IOException; //Read image file from local

	public abstract byte[] getImage(String filename) throws IOException; //Get image file from HDFS

}
