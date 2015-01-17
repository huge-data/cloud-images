package zx.soft.images.mapred;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import zx.soft.images.bundle.CloudImageBundle;
import zx.soft.images.bundle.ImageHead;
import zx.soft.images.bundle.ImageIndex;
import zx.soft.images.codec.RawImage;

public class ImageBundleInputFormat extends CombineFileInputFormat<ImageHead, RawImage> {

	int sum;
	int block_size;

	// Constructor
	public ImageBundleInputFormat() {
		this.sum = 1;
		this.block_size = 128;
	}

	// RecordReader
	@Override
	public RecordReader<ImageHead, RawImage> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		CombineFileSplit cfs = (CombineFileSplit) split;
		ImageBundleRecordReader irb = new ImageBundleRecordReader(cfs, context);
		try {
			irb.initialize(cfs, context);
		} catch (Exception e) {
		}
		return irb;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long start_pos, end_pos, split_size;
		ArrayList<Long> startlist;
		ArrayList<Long> offsetlist;
		ArrayList<String> hosts;
		ArrayList<Path> pathlist;
		Configuration conf = job.getConfiguration();
		for (FileStatus file : listStatus(job)) {
			String b_name = file.getPath().getName();
			int dot = b_name.lastIndexOf('_');
			String l_name = b_name.substring(dot + 1);
			if ((b_name.equals("_SUCCESS")) || (b_name.substring(0, 4).equals("part")) || (l_name.equals("num"))
					|| (l_name.equals("dat"))) {
				continue;
			}
			//			System.out.println("Get Splits");	        
			System.out.println("Filename is " + b_name);
			start_pos = end_pos = split_size = 0;
			startlist = new ArrayList<Long>();
			offsetlist = new ArrayList<Long>();
			hosts = new ArrayList<String>();
			pathlist = new ArrayList<Path>();
			ImageIndex ii = new ImageIndex("1", 1, 1, 1, 0, 0, 0, 0, "username", "filename", null, null, "memo");
			Path path = file.getPath();
			//			System.out.println(path.toString());
			FileSystem fs = path.getFileSystem(conf);
			// Find how many image files in a bundle

			DataInputStream is_num = new DataInputStream(fs.open(new Path(file.getPath().toString() + "_num")));
			sum = is_num.readInt() - 1;
			is_num.close();

			CloudImageBundle cib = new CloudImageBundle(path, sum, conf);
			cib.open(0);
			while (sum >= 0) {
				ii.read((DataInputStream) cib.getIndexReadStream());
				//				ii.print_row();
				//				ii.print();
				start_pos = ii.getOffset();
				// ImageHead's size is 24
				// Modified on 07/14
				end_pos = start_pos + 24 + 2 + ii.getFilename().length() + ii.getLength() - 1;
				//				System.out.println("ID = " + sum + "start_pos="+start_pos+"\n end_pos="+end_pos);
				if (selector(ii) && (ii.getValid() == 1)) {
					split_size += end_pos - start_pos + 1;
					startlist.add(start_pos);
					offsetlist.add(end_pos - start_pos + 1);
					//					Path data_path = new Path(path + "_dat");
					pathlist.add(path);
					/*					BlockLocation[] blkLocations = fs.getFileBlockLocations(fs.get(conf).getFileStatus(data_path), start_pos, end_pos);
										int startIndex = getBlockIndex(blkLocations, start_pos);
										int endIndex = getBlockIndex(blkLocations, end_pos);
										for (int i = startIndex; i <= endIndex; i++) {
											String[] blkHosts = blkLocations[i].getHosts();
											for (int j = 0; j < blkHosts.length; j++) {
												int found = 1;
												for (int k = 0; k < hosts.size(); k++) {
													if (hosts.get(k).equals(blkHosts[j])) {
														found = 0;
														break;
													}
												}
												if (found==0) {
													hosts.add(blkHosts[j]);
												}
											}
										}	
					*/
				}
				//				System.out.println("Split_size = " + split_size);
				sum--;
				if ((split_size > block_size * 1024 * 1024) || ((sum < 0) && (split_size != 0))) {
					System.out.println(pathlist.size());
					System.out.println(startlist.size());
					System.out.println(offsetlist.size());
					System.out.println(hosts.size());
					Path[] paths_Array = pathlist.toArray(new Path[pathlist.size()]);
					long[] starts_Array = ArrayUtils.toPrimitive(startlist.toArray(new Long[startlist.size()]));
					long[] offsets_Array = ArrayUtils.toPrimitive(offsetlist.toArray(new Long[offsetlist.size()]));
					String[] hosts_Array = hosts.toArray(new String[hosts.size()]);
					splits.add(new CombineFileSplit(paths_Array, starts_Array, offsets_Array, hosts_Array));
					pathlist = null;
					startlist = null;
					offsetlist = null;
					hosts = null;
					pathlist = new ArrayList<Path>();
					startlist = new ArrayList<Long>();
					offsetlist = new ArrayList<Long>();
					hosts = new ArrayList<String>();
					split_size = 0;
				}
			}
			cib.open_close();
		}
		System.out.println("# of splits = " + splits.size());
		return splits;
	}

	public boolean selector(ImageIndex ii) {
		return true;
	}

}
