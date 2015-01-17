package zx.soft.images.process;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
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

/* Description of Cropping Alogrithm as bellow
 * 
 * Parameters Needed: 	the Cropping Starting Position --crop_x crop_y
 * 			
 *			crop_x stands for the row number of starting cropping pixels
 *			crop_y stands for the colum number of starting cropping pixels
 * 			width stands for the width of new image from starting position
 *			height stands for the height of new image from starting position
 *
 *			For the above parameters, the custumer can set according to their needs
 *
 * 
 * Parameters Config:	in CP_kernel(file exists in the bin directory)
 *			
 *			ONLY need write 4 parameters, they are crop_x, crop_y, new_width, new_height, respectively.
 * 			
 * 			now the version can not support the parameters in the multiple lines or mutiple combined parameters in different lines
 *
 *  
*/
@SuppressWarnings("unused")
public class Cropping extends Configured implements Tool {

	private static Path path_num = null;
	private static int sum = 0;
	private static int sum_int = 0;

	private static DataOutputStream data_write_stream = null;
	private static DataOutputStream index_write_stream = null;

	public static class Cropping_Map extends Mapper<ImageHead, RawImage, IntWritable, RawImage> {

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
			Configuration conf = context.getConfiguration();
			int index = key.getIndex();
			int imageWidth = value.getWidth();
			int imageHeight = value.getHeight();
			int totalPixels = imageWidth * imageHeight;
			int[] R = value.getR();
			int[] G = value.getG();
			int[] B = value.getB();
			int[] Y = value.getY();
			int[] Cb = value.getCb();
			int[] Cr = value.getCr();
			RawImage image;

			int crop_x = Integer.parseInt(conf.get("crop_x"));
			int crop_y = Integer.parseInt(conf.get("crop_y"));
			int new_width = Integer.parseInt(conf.get("new_width"));
			int new_height = Integer.parseInt(conf.get("new_height"));
			int new_totalPixels = 0;

			// check if the parameters is valid 

			if ((crop_x >= 0 && crop_x <= imageWidth) && (crop_y >= 0 && crop_y <= imageHeight)) // crop_x and crop_y both valid
			{

				if ((crop_x + new_width) > imageWidth) { // retrieving image's width exceed raw image

					if ((crop_y + new_height) > imageHeight) { // retrieving image's height exceed raw image

						new_totalPixels = (imageWidth - crop_x) * (imageHeight - crop_y);
						imageWidth = imageWidth - crop_x;
						imageHeight = imageHeight - crop_y;

					} else {
						new_totalPixels = (imageWidth - crop_x) * new_height;
						imageWidth = imageWidth - crop_x;
						imageHeight = new_height;
					}
				} else {
					if ((crop_y + new_height) > imageHeight) {

						new_totalPixels = new_width * (imageHeight - crop_y);
						imageWidth = new_width;
						imageHeight = imageHeight - crop_y;

					} else {
						new_totalPixels = new_width * new_height;
						imageWidth = new_width;
						imageHeight = new_height;
					}
				}

				int[] R_new = new int[new_totalPixels];
				int[] G_new = new int[new_totalPixels];
				int[] B_new = new int[new_totalPixels];
				int[] Y_new = new int[new_totalPixels];
				int[] Cb_new = new int[new_totalPixels];
				int[] Cr_new = new int[new_totalPixels];

				// System.out.println("totalPixels is " + totalPixels+ "\n" + "input Pixels is " + new_totalPixels +"\n");

				// src_src_basis is the calculating position that retrieving source image pixels information
				int src_basis = crop_x * imageWidth + crop_y;

				//copy the data from raw image array to new array
				/*for(int  i = 0; i < new_totalPixels && src_basis <= totalPixels ; i++,src_basis++){
					R_new[i] = R[src_basis];
					G_new[i] = G[src_basis];
					B_new[i] = B[src_basis];
					Y_new[i] = Y[src_basis];
					Cb_new[i] = Cb[src_basis];
					Cr_new[i] = Cr[src_basis];	
				
				}*/

				//different way and different perform ,below is better and easy to understant sometime

				//copy the data from raw image array to new array 
				System.arraycopy(R, src_basis, R_new, 0, new_totalPixels);
				System.arraycopy(G, src_basis, G_new, 0, new_totalPixels);
				System.arraycopy(B, src_basis, B_new, 0, new_totalPixels);
				System.arraycopy(Y, src_basis, Y_new, 0, new_totalPixels);
				System.arraycopy(Cb, src_basis, Cb_new, 0, new_totalPixels);
				System.arraycopy(Cr, src_basis, Cr_new, 0, new_totalPixels);

				image = new RawImage(imageWidth, imageHeight, R_new, G_new, B_new, Y_new, Cb_new, Cr_new);
				System.out.println("create RawImage succeed\n new_width \n" + new_width + "new height \n" + new_height
						+ "\n " + "totalpix" + totalPixels);
			} else {
				image = new RawImage(imageWidth, imageHeight, R, G, B, Y, Cb, Cr);
				System.out.println("Do not Change X Y are wrong");
			}

			// Write a image into bundles.
			byte[] buf = null;
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			buf = codec.imageEncoder(image, "JPG", bos, 1);
			System.out.println("The file name is " + index);

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

	//	public static class Cropping_Reduce extends Reducer<IntWritable, RawImage, IntWritable, IntWritable> {
	//		public void reduce(IntWritable key, Iterable<RawImage> values, Context context) throws IOException, InterruptedException{
	//		}
	//	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		File cropping = new File(args[2]);
		BufferedReader reader = new BufferedReader(new FileReader(cropping));
		String line = null;
		String crop_x = null;
		String crop_y = null;
		String new_width = null;
		String new_height = null;
		while ((line = reader.readLine()) != null) {
			System.out.println("The cropping info is " + line);
			String[] strS = line.split(" ");
			crop_x = strS[0];
			crop_y = strS[1];
			new_width = strS[2];
			new_height = strS[3];
		}
		reader.close();

		conf.set("crop_x", crop_x);
		conf.set("crop_y", crop_y);
		conf.set("new_width", new_width);
		conf.set("new_height", new_height);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Cropping");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RawImage.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);
		job.setJarByClass(Cropping.class);
		job.setMapperClass(Cropping_Map.class);
		//		job.setReducerClass(Cropping_Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println("The crop_x is " + crop_x);
		System.out.println("The crop_y is " + crop_y);
		System.out.println("The input width is " + new_width);
		System.out.println("The input height is " + new_height);
		System.out.println("Start job.waitForCompletion");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("After job.waitForCompletion");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Cropping(), args);
		System.exit(exitCode);
	}

}
