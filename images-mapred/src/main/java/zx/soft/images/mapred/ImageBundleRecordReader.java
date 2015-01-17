package zx.soft.images.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import zx.soft.images.bundle.CloudImageBundle;
import zx.soft.images.bundle.ImageHead;
import zx.soft.images.codec.Codec;
import zx.soft.images.codec.RawImage;

public class ImageBundleRecordReader extends RecordReader<ImageHead, RawImage> {

	private Configuration conf;
	private CloudImageBundle cib;
	private int i, sum;
	private CombineFileSplit cfs;
	private ImageHead ih;
	private Codec codec;
	private long current_offset = 0;

	public ImageBundleRecordReader(CombineFileSplit cfs, TaskAttemptContext context) {
		super();
		this.cfs = cfs;
		this.i = 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		cfs = (CombineFileSplit) split;
		//		Path path = cfs.getPath(0);
		//		FileSystem fs = path.getFileSystem(conf);
		sum = cfs.getNumPaths();
		cib = new CloudImageBundle(cfs.getPath(0), conf);
		System.out.println(cfs.getPath(0));
		codec = new Codec();
		//For read
		cib.open(0);
		cib.getInputStream().skip(cfs.getOffset(0));
		System.out.println("recordreader init!");
	}

	@Override
	public void close() throws IOException {
		cib.open_close();
	}

	@Override
	public ImageHead getCurrentKey() throws IOException, InterruptedException {
		ih = new ImageHead(1, 1, 1, 1, 1, "a");
		ih.read(cib.getInputStream());
		System.out.println(ih.imageType + " " + ih.index + " " + ih.width + " " + ih.height + " " + ih.size);
		return ih;
	}

	@Override
	public RawImage getCurrentValue() throws IOException, InterruptedException {
		byte[] image_byte = new byte[(int) (ih.getSize())];
		try {
			cib.getInputStream().readFully(image_byte, 0, (int) (ih.getSize()));
			System.out.println("Image Index is " + ih.getIndex());
			System.out.println("Image Size is " + ih.getSize());
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		InputStream is = new ByteArrayInputStream(image_byte);
		return codec.imageDecoder(is, 1);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ((float) i) / sum;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		System.out.println("Sum is " + sum);
		System.out.println("Value of i is " + i);
		if (i >= sum)
			return false;
		System.out.println("old position is " + current_offset);
		//		cib.seek(current_offset, cfs.getOffset(i));
		//		DataInputStream ds = cib.getInputStream();
		System.out.println("CFS offset = " + cfs.getOffset(i));
		current_offset = cfs.getOffset(i);
		System.out.println("new position is " + current_offset);
		i++;
		return true;
	}

}
