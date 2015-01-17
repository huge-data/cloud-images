package zx.soft.images.process;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.images.bundle.ImageHead;
import zx.soft.images.bundle.ImageIndex;
import zx.soft.images.codec.Codec;
import zx.soft.images.codec.RawImage;
import zx.soft.images.mapred.ImageBundleInputFormat;

@SuppressWarnings("unused")
public class ImageGradient extends Configured implements Tool {

	private static int _imageWidth;
	private static int _imageHeight;

	private static int sum_int = 0;
	private static Path path_num = null;

	private static DataOutputStream data_write_stream = null;
	private static DataOutputStream index_write_stream = null;

	public static class IG_Map extends Mapper<ImageHead, RawImage, IntWritable, RawImage> {

		public FileSystem fs = null;
		public Codec codec = null;
		public String outputDir = null;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			fs = FileSystem.get(conf);
			codec = new Codec();
			String outputDir = TextOutputFormat.getOutputPath(context).toString();
			String taskId = conf.get("mapred.task.id");
			Path file_path = new Path(outputDir + "/" + taskId + "_dat");
			Path index_path = new Path(outputDir + "/" + taskId);
			index_write_stream = fs.create(index_path);
			index_write_stream.writeInt(0x00434942); //Write a magic code
			data_write_stream = fs.create(file_path);
		}

		@Override
		public void map(ImageHead key, RawImage value, Context context) throws IOException, InterruptedException {
			int index = key.getIndex();
			int imageWidth = value.getWidth();
			int imageHeight = value.getHeight();
			int totalPixels = imageWidth * imageHeight;
			int[] Y = value.getY();
			//			int[] newY = new int[Y.length];
			int[][] gradient = new int[imageHeight][imageWidth];
			int[][] angles = new int[imageHeight][imageWidth];
			int Gx, Gy;
			int GxMask[][] = { { -1, 0, 1 }, { -2, 0, 2 }, { -1, 0, 1 } };
			int GyMask[][] = { { 1, 2, 1 }, { 0, 0, 0 }, { -1, -2, -1 } };
			final int Threshold = 20;

			// Calculate the angles of each pixel
			// Suppose we have the Gaussian Blurred RawImage already
			for (int i = 1; i < imageHeight - 1; i++) {
				for (int j = 1; j < imageWidth - 1; j++) {
					Gx = Gy = 0;
					for (int ii = -1; ii <= 1; ii++) {
						for (int jj = -1; jj <= 1; jj++) {
							Gx += Y[(i + ii) * imageWidth + j + jj] * GxMask[ii + 1][jj + 1];
							Gy += Y[(i + ii) * imageWidth + j + jj] * GyMask[ii + 1][jj + 1];
						}
					}
					gradient[i][j] = (int) Math.sqrt(Math.pow(Gx, 2.0) + Math.pow(Gy, 2.0));
				}
			}
			for (int i = 1; i < imageHeight - 1; i++) {
				for (int j = 1; j < imageWidth - 1; j++) {
					Y[i * imageWidth + j] = gradient[i][j];
				}
			}

			RawImage image = new RawImage(imageWidth, imageHeight, value.getR(), value.getG(), value.getB(), Y,
					value.getCb(), value.getCr());

			// Write a image into bundles.
			byte[] buf = null;
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			buf = codec.imageEncoder(image, "JPG", bos, 1);
			System.out.println("The file name is " + index);

			Configuration conf = context.getConfiguration();

			sum_int++;

			// For ImageIndex
			Date date = new Date();
			SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss");
			String create_time = dateformat.format(date);
			String modify_time = create_time;
			UUID uuid = UUID.randomUUID();
			long offset = ((FSDataOutputStream) data_write_stream).getPos();

			ImageIndex ii = new ImageIndex("1", 1, 1, 1, imageWidth, imageHeight, 0, 0, "username", "filename", null,
					null, "memo");
			ii.setIID(uuid.toString());
			ii.setIndex(sum_int);
			ii.setCreate_time(create_time);
			ii.setModify_time(modify_time);
			ii.setOffset(offset);
			ii.setLength(buf.length);

			// Get the file's name
			String filename = key.getFilename();
			ii.setFilename(filename);
			/*
			DataInputStream index_read_stream = null;
			CombineFileSplit inputSplit = (CombineFileSplit)context.getInputSplit();
			Path inputDir = inputSplit.getPath(0);
			index_read_stream = new DataInputStream(fs.open(inputDir));			
			index_read_stream.readInt();
			ImageIndex iii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
			while(true) {
				iii.read(index_read_stream);
				if (iii.getIndex() == index) {
					ii.setFilename(iii.getFilename());	
					System.out.println("The file name is " + ii.getFilename());
					break;
				}
			}
			index_read_stream.close();
			*/

			ii.write(index_write_stream);

			// For ImageHead
			ImageHead ih = new ImageHead(sum_int, 1, imageWidth, imageHeight, buf.length, filename);
			ih.write(data_write_stream);
			data_write_stream.write(buf);

			//			index_write_stream.close();
			//			data_write_stream.close();

			//System.gc();

			//context.write(new IntWritable(index), value);
		}

		@SuppressWarnings("rawtypes")
		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException {

			Configuration conf = context.getConfiguration();
			fs = FileSystem.get(conf);

			data_write_stream.close();
			index_write_stream.close();

			String outputDir = TextOutputFormat.getOutputPath(context).toString();
			String taskId = conf.get("mapred.task.id");

			Path path_num = new Path(outputDir + "/" + taskId + "_num");
			DataOutputStream dos = fs.create(path_num);
			dos.writeInt(sum_int);
			dos.close();
		}
	}

	//	public static class IG_Reduce extends Reducer<IntWritable, RawImage, IntWritable, IntWritable> {
	//
	//		public void reduce(IntWritable key, Iterable<RawImage> values, Context context) throws IOException, InterruptedException{
	//		}
	//	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "ImageGradient");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RawImage.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setJarByClass(ImageGradient.class);
		job.setMapperClass(IG_Map.class);
		//		job.setReducerClass(IG_Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ImageGradient(), args);
		//		System.exit(exitCode);
	}

}
