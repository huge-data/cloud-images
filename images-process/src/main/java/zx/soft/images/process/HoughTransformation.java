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
public class HoughTransformation extends Configured implements Tool {

	private static Path path_num = null;
	private static int sum_int = 0;

	private static DataOutputStream data_write_stream = null;
	private static DataOutputStream index_write_stream = null;

	public static class HoughTransformation_Map extends Mapper<ImageHead, RawImage, IntWritable, RawImage> {
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
							Gy += Y[(i + ii) * imageWidth + j + jj] * GyMask[ii = 1][jj + 1];
						}
					}
					gradient[i][j] = (int) Math.sqrt(Math.pow(Gx, 2.0) + Math.pow(Gy, 2.0));
					double angel = (Math.atan2(Gx, Gy) / 3.14159) * 180.0;
					if (((angel < 22.5) && (angel > -22.5)) || (angel > 157.5) || (angel < -157.5))
						angles[i][j] = 0;
					else if (((angel > 22.5) && (angel < 67.5)) || ((angel < -112.5) && (angel > -157.5)))
						angles[i][j] = 45;
					else if (((angel > 67.5) && (angel < 112.5)) || ((angel < -67.5) && (angel > -112.5)))
						angles[i][j] = 90;
					else if (((angel > 112.5) && (angel < 157.5)) || ((angel < -22.5) && (angel > -67.5)))
						angles[i][j] = 135;
				}
			}
			// set pixel to white if an edge is formed by both its neighbor and itself
			for (int i = 2; i < imageHeight - 1; i++) {
				for (int j = 2; j < imageWidth - 1; j++) {
					// set Upper Threashold to 60, Lower Threshold to 30.
					if (gradient[i][j] > Threshold) {
						//						System.out.println("In map. Has some gradient larger than 60 -" + gradient[i][j] + ", the pixel is: height-" + i + " width-" + j);
						switch (angles[i][j]) {
						case 0:
							if ((angles[i][j + 1] == 0) && (gradient[i][j + 1] > Threshold)) {
								Y[i * imageWidth + j] = Y[i * imageWidth + j + 1] = 255;
							}
							break;
						case 45:
							if ((angles[i + 1][j + 1] == 45) && (gradient[i + 1][j + 1] > Threshold)) {
								Y[i * imageWidth + j] = Y[(i + 1) * imageWidth + j + 1] = 255;
							}
							break;
						case 90:
							if ((angles[i + 1][j] == 90) && (gradient[i + 1][j] > Threshold)) {
								Y[i * imageWidth + j] = Y[(i + 1) * imageWidth + j] = 255;
							}
							break;
						case 135:
							if ((angles[i + 1][j - 1] == 135) && (gradient[i + 1][j - 1] > Threshold)) {
								Y[i * imageWidth + j] = Y[(i + 1) * imageWidth + j - 1] = 255;
							}
							break;
						}
					} else {
						Y[i * imageWidth + j] = 0;
					}
				}
			}
			for (int i = 0; i < imageHeight * imageWidth; i++) {
				if (Y[i] != 0 && Y[i] != 255)
					Y[i] = 0;
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

	public void houghTransformation(RawImage image) {
		int image_h = image.getHeight();
		int image_w = image.getWidth();

		int houghTransformation_height = (int) (Math.sqrt(2) * Math.max(image_h, image_w)) / 2;

		int houghHeight = 2 * houghTransformation_height;

		int[][] houghTransformMatrix = new int[180][houghHeight];

		int centerX = image_w / 2;
		int centerY = image_h / 2;
		System.out.println("Before big loop");
		System.out.println("Image_h:" + image_h);
		System.out.println("Image_w:" + image_w);
		int points = 0;
		/*			for (int y = 0; y<image_h; y++){
						for(int x = 0; x < image_w; x++){
							int[] img_value = image.getR();
							System.out.println("img_value: "+img_value);
							if(img_value[x*y+x] > 250){
								for(int t=0; t<180; t++){
									int r = (int) (((x - centerX) * Math.cos(t)) + ((y - centerY) * Math.sin(t))); 
									r += houghHeight;
									if(r < 0 || r >= houghHeight) continue;
									
									houghTransformMatrix[t][r]++;
								}
								points++;
							}
						}
					}*/
	}

	//	public static class HoughTransformation_Reduce extends Reducer<IntWritable, RawImage, IntWritable, IntWritable> {
	//
	//		public void setup(Context context) throws IOException {
	//		}
	//
	//		public void reduce(IntWritable key, Iterable<RawImage> values, Context context) throws IOException, InterruptedException{
	//
	//		}
	//	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HoughTransformation");
		//		job.setJobName("HistogramEqualization");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RawImage.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);
		//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(HoughTransformation.class);
		job.setMapperClass(HoughTransformation_Map.class);
		//		job.setCombinerClass(Reduce.class);
		//		job.setReducerClass(HoughTransformation_Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println("Start job.waitForCompletion");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("After job.waitForCompletion");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HoughTransformation(), args);
		System.exit(exitCode);
	}

}
