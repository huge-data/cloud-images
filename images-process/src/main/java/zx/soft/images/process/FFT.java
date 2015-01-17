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
public class FFT extends Configured implements Tool {

	private static Path path_num = null;
	private static int sum = 0;
	private static int sum_int = 0;

	private static DataOutputStream data_write_stream = null;
	private static DataOutputStream index_write_stream = null;

	//	private static Complex[] TD = null;
	//	private static Complex[] FD = null;
	//	private static Complex[] W = null;

	public static class FFT_Map extends Mapper<ImageHead, RawImage, IntWritable, RawImage> {

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
			int[] Y = value.getY();
			int[] Cr = value.getCr();
			int[] Cb = value.getCb();
			int w = 1;
			int h = 1;
			int wp = 0;
			int hp = 0;
			// Compute w & h 's computation times
			while (w < imageWidth) {
				w *= 2;
				wp++;
			}
			while (h < imageHeight) {
				h *= 2;
				hp++;
			}

			int logN = wp + hp;
			int[] tmp = new int[w * h];
			int[] R = new int[imageWidth * imageHeight];
			int[] G = new int[imageWidth * imageHeight];
			int[] B = new int[imageWidth * imageHeight];
			FFTOP fftop = new FFTOP(logN);

			float[] real = new float[w * h];
			float[] imag = new float[w * h];

			Init(real, imag, Y, imageWidth, imageHeight);
			fftop.transform2D(real, imag, w, h, true);

			//		    System.out.println("Finish 2D FFT computation");

			// Compute Magnitude
			double max = 0;
			for (int i = 0; i < h; i++)
				for (int j = 0; j < w; j++) {
					double X2 = real[i * w + j];
					double Y2 = imag[i * w + j];
					double Z = X2 * X2 + Y2 * Y2;
					if (max < Z)
						max = Z;
				}

			double c = 255 / Math.log10(1 + max);

			for (int i = 0; i < Y.length; i++) {
				tmp[i] = 0;
			}

			int[] newY = new int[w * h];

			for (int i = 0; i < h; i++) {
				for (int j = 0; j < w; j++) {
					double X2 = real[i * w + j];
					double Y2 = imag[i * w + j];
					//		double Z = Math.sqrt(X2*X2 + Y2*Y2);
					double Z = X2 * X2 + Y2 * Y2;
					//		int intZ = (int)(Z/100);
					int intZ = (int) (c * Math.log10(1 + Z));
					if (intZ > 255)
						intZ = 255;
					if (intZ < 0)
						intZ = 0;
					tmp[i * w + j] = intZ;
					newY[i * w + j] = intZ;
				}
			}

			// Change to new Graph
			// 3 to 1, 2 to 4
			for (int i = h / 2; i < h; i++) {
				System.arraycopy(tmp, i * w, newY, (i - h / 2) * w + w / 2, w / 2);
				System.arraycopy(tmp, i * w + w / 2, newY, (i - h / 2) * w, w / 2);
			}

			// 1 to 3, 4 to 2
			for (int i = 0; i < h / 2; i++) {
				System.arraycopy(tmp, i * w, newY, (i + h / 2) * w + w / 2, w / 2);
				System.arraycopy(tmp, i * w + w / 2, newY, (i + h / 2) * w, w / 2);
			}
			/*
			for (int i = 0; i < Y.length; i++) {
				System.out.println(tmp + " = "+ tmp[i]);
			}
			*/
			// Change R, G and B

			//Get the same number of pixels.
			int[] newFFT = new int[imageWidth * imageHeight];
			for (int i = 0; i < imageHeight; i++) {
				for (int j = 0; j < imageWidth; j++) {
					newFFT[i * imageWidth + j] = newY[((h - imageHeight) / 2 + i) * w + j + (w - imageWidth) / 2];
				}
			}

			RawImage image = new RawImage(imageWidth, imageHeight, R, G, B, null, null, null);

			//			System.arraycopy( newY, 0, R, 0, R.length);
			//			System.arraycopy( newY, 0, G, 0, G.length);
			//			System.arraycopy( newY, 0, B, 0, B.length);

			image.setR(newFFT);
			image.setG(newFFT);
			image.setB(newFFT);

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

	//	public static class FFT_Reduce extends Reducer<IntWritable, RawImage, IntWritable, IntWritable> {
	//
	//		public void reduce(IntWritable key, Iterable<RawImage> values, Context context) throws IOException, InterruptedException{
	//		}
	//	}

	private static void Init(float[] real, float[] imag, int[] Y, int Width, int Height) {
		int w = 1;
		int h = 1;
		int wp = 0;
		int hp = 0;
		int max = 0;
		double angle;
		int N;

		// Compute w & h 's computation times
		while (w < Width) {
			w *= 2;
			wp++;
		}
		while (h < Height) {
			h *= 2;
			hp++;
		}

		// Modify Data

		// Data Modify; Use 0 fill in non-exsiting data.
		for (int i = 0; i < h; i++) {
			if ((i > (h - Height) / 2 - 1) && (i < (h - Height) / 2 + Height)) {
				for (int j = 0; j < w; j++) {
					if ((j > (w - Width) / 2 - 1) && (j < (w - Width) / 2 + Width)) {
						real[i * w + j] = Y[(i - (h - Height) / 2) * Width + j - (w - Width) / 2];
						imag[i * w + j] = 0;
					} else {
						real[i * w + j] = 0;
						imag[i * w + j] = 0;
					}
				}
			} else {
				for (int j = 0; j < w; j++) {
					real[i * w + j] = 0;
					imag[i * w + j] = 0;
				}
			}
		}
	}

	// Copy a new Image
	public static void copyToNew(int[] source, int[] dest, int sourceX, int sourceY, int destX, int destY, int width,
			int height) {
		for (int i = sourceY; i < sourceY + height; i++)
			for (int j = sourceX; j < sourceX + width; j++) {
				dest[(destY + i) * 2 * width + destX + j] = source[i * 2 * width + j];
			}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "FFT");
		job.setJobName("FFT");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RawImage.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(FFT.class);
		job.setMapperClass(FFT_Map.class);
		//		job.setReducerClass(FFT_Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println("Start job.waitForCompletion");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("After job.waitForCompletion");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FFT(), args);
		System.exit(exitCode);
	}

}
