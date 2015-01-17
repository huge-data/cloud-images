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

@SuppressWarnings("unused")
public class Convolution extends Configured implements Tool {

	private static Path path_num = null;
	private static int sum = 0;
	private static int sum_int = 0;

	private static DataOutputStream data_write_stream = null;
	private static DataOutputStream index_write_stream = null;

	public static class CF_Map extends Mapper<ImageHead, RawImage, IntWritable, RawImage> {

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
			int[] R_new = new int[totalPixels];
			int[] G_new = new int[totalPixels];
			int[] B_new = new int[totalPixels];
			double R_temp, G_temp, B_temp;
			//			double[][] gaussian_matrix = {{0.0585498, 0.0965324, 0.0585498}, {0.0965324, 0.159155, 0.0965324}, {0.0585498, 0.0965324, 0.0585498}};			
			//			double[][] gaussian_matrix = {{0.0625, 0.125, 0.0625}, {0.125, 0.25, 0.125}, {0.0625, 0.125, 0.0625}};
			//			double[][] gaussian_matrix = {{2, 4, 5, 4, 2}, {4, 9, 12, 9, 4}, {5, 12, 15, 12, 5}, {4, 9, 12, 9, 4}, {2, 4, 5, 4, 2}};
			//			for (int i = 0; i < 5; i++) {
			//				for (int j = 0; j < 5; j++) {
			//					gaussian_matrix[i][j] = gaussian_matrix[i][j] / 159;
			//				}
			//			}

			// Get matrix and convolution type
			String matrix = conf.get("matrix");
			int mtype = Integer.parseInt(conf.get("mtype"));
			int mwidth = Integer.parseInt(conf.get("mwidth"));
			int mheight = Integer.parseInt(conf.get("mheight"));
			int[][] matrix_0 = new int[mheight][mwidth];
			int[][] matrix_1 = new int[mheight][mwidth];
			String[] strS = matrix.split(" ");
			int matrix_sum = 0;
			for (int i = 0; i < mheight; i++)
				for (int j = 0; j < mwidth; j++) {
					matrix_0[i][j] = Integer.parseInt(strS[i * mwidth + j]);
					matrix_1[mheight - i - 1][mwidth - j - 1] = matrix_0[i][j];
					matrix_sum += matrix_0[i][j];
				}

			int matrix_h_bound = mheight / 2;
			int matrix_w_bound = mwidth / 2;

			int matrix_pointer = 1;
			int[][] gaussian_matrix = matrix_0;

			for (int i = matrix_h_bound; i < imageHeight - matrix_h_bound; i++) {
				for (int j = matrix_w_bound; j < imageWidth - matrix_w_bound; j++) {
					R_temp = 0.0;
					G_temp = 0.0;
					B_temp = 0.0;
					for (int ii = -matrix_h_bound; ii <= matrix_h_bound; ii++) {
						for (int jj = -matrix_w_bound; jj <= matrix_w_bound; jj++) {
							if (mtype == 1) {
								if (matrix_pointer == 1) {
									gaussian_matrix = matrix_0;
									matrix_pointer = 0;
								} else {
									gaussian_matrix = matrix_1;
									matrix_pointer = 1;
								}
							}
							R_temp += R[(i + ii) * imageWidth + j + jj]
									* gaussian_matrix[ii + matrix_h_bound][jj + matrix_w_bound];
							G_temp += G[(i + ii) * imageWidth + j + jj]
									* gaussian_matrix[ii + matrix_h_bound][jj + matrix_w_bound];
							B_temp += B[(i + ii) * imageWidth + j + jj]
									* gaussian_matrix[ii + matrix_h_bound][jj + matrix_w_bound];
						}
					}
					R_new[i * imageWidth + j] = (int) (R_temp / matrix_sum);
					G_new[i * imageWidth + j] = (int) (G_temp / matrix_sum);
					B_new[i * imageWidth + j] = (int) (B_temp / matrix_sum);
				}
			}

			RawImage image = new RawImage(imageWidth, imageHeight, R_new, G_new, B_new, value.getY(), value.getCb(),
					value.getCr());

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

	//	public static class CF_Reduce extends Reducer<IntWritable, RawImage, IntWritable, IntWritable> {
	//		public void reduce(IntWritable key, Iterable<RawImage> values, Context context) throws IOException, InterruptedException{
	//		}
	//	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		File matrix = new File(args[2]);
		BufferedReader reader = new BufferedReader(new FileReader(matrix));
		String line = null;
		String matrix_value = "";
		String mwidth = null;
		String mheight = null;
		int level = 0;
		while ((line = reader.readLine()) != null) {
			String[] strS = line.split(" ");
			if (level == 0) {
				mheight = strS[0];
				level++;
			} else if (level == 1) {
				mwidth = strS[0];
				level++;
			} else {
				for (int i = 0; i < strS.length; i++) {
					matrix_value = matrix_value + strS[i] + " ";
				}
			}
		}
		reader.close();

		String mtype = new String(args[3]);
		System.out.println(matrix_value);
		conf.set("matrix", matrix_value);
		conf.set("mwidth", mwidth);
		conf.set("mheight", mheight);
		conf.set("mtype", mtype);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Convolution");
		//		job.setJobName("HistogramEqualization");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RawImage.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);
		//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(Convolution.class);
		job.setMapperClass(CF_Map.class);
		//		job.setCombinerClass(Reduce.class);
		//		job.setReducerClass(CF_Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println("The type is " + mtype);
		System.out.println("The width is " + mwidth);
		System.out.println("The height is " + mheight);
		System.out.println("Start job.waitForCompletion");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("After job.waitForCompletion");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Convolution(), args);
		System.exit(exitCode);
	}

}
