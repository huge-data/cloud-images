package zx.soft.images.bundle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CloudImageBundleImplement {

	public static void main(String[] args) {

		int index = 0;
		int changed = 0;
		Configuration conf = null;
		FileSystem fs = null;
		Path path_num = null;

		if (args[1].equals("-r")) {
			path_num = new Path(args[2] + "_num");
		} else {
			path_num = new Path(args[1] + "_num");
		}

		try {
			conf = new Configuration();
			fs = FileSystem.get(conf);
			// Check whether the bunle exsits 
			if (fs.exists(path_num)) {
				DataInputStream is_num = new DataInputStream(fs.open(path_num));
				index = is_num.readInt();
				is_num.close();
			}
		} catch (Exception e) {
			//
		}

		if (args.length == 0) {
			System.out.println("Sorry, no other operations now. Will update soon.\n");
		} else if (args[0].equals("add")) {
			if (args[1].equals("-r")) {
				try {
					String bundle_string = args[2];
					Path bundle_path = new Path(bundle_string);
					File directory = new File(args[3]);
					File[] files = directory.listFiles();
					CloudImageBundle cib = new CloudImageBundle(bundle_path, index, conf);
					cib.open(1);
					for (int i = 0; i < files.length; i++) {
						if (files[i].isFile()) {
							String image_path = args[3] + "/" + files[i].getName();
							cib.addImage(image_path, index);
							index++;
							changed = 1;
						}
					}
					cib.open_close();
				} catch (IOException e) {
					System.out.println(e.toString());
				}
			} else {
				try {
					String bundle_string = args[1];
					Path bundle_path = new Path(bundle_string);
					String image_path = args[2];
					CloudImageBundle cib = new CloudImageBundle(bundle_path, index, conf);
					cib.open(1);
					cib.addImage(image_path);
					cib.open_close();
					System.out.println("Successfully Add one image in Bundle.\n");
					index++;
					changed = 1;
				} catch (IOException e) {
					System.out.println(e.toString());
				}
			}
		} else if (args[0].equals("get")) {
			if (args[1].equals("-r")) {
				try {
					Path dir_path = new Path(args[2]);
					FileStatus[] files = fs.listStatus(dir_path);

					for (FileStatus file : files) {
						String b_name = file.getPath().getName();
						int dot = b_name.lastIndexOf('_');
						String l_name = b_name.substring(dot + 1);
						if ((b_name.equals("_SUCCESS")) || (b_name.substring(0, 4).equals("part"))
								|| (l_name.equals("num")) || (l_name.equals("dat"))) {
							continue;
						}

						Path bundle_path = file.getPath();
						path_num = new Path(file.getPath().toString() + "_num");
						DataInputStream is_num = new DataInputStream(fs.open(path_num));
						index = is_num.readInt();
						is_num.close();

						String dir_name = args[3];
						System.out.println("Index = " + index);
						for (int i = 0; i < index; i++) {
							CloudImageBundle cib = new CloudImageBundle(bundle_path, index, conf);
							byte[] image_data = cib.getImage(i);
							System.out.println("The filename is " + cib.getFilename());
							FileOutputStream o = new FileOutputStream(dir_name + "/" + cib.getFilename());
							o.write(image_data);
							o.close();
							System.out.println("Successfully Get the " + i + "th file from Bundle.\n");
							cib.open_close();
						}
					}
				} catch (IOException e) {
					System.out.println(e.toString());
				}
			} else {
				try {
					Path bundle_path = new Path(args[1]);
					String file_name = args[2];
					CloudImageBundle cib = new CloudImageBundle(bundle_path, conf);
					byte[] image_data = cib.getImage(file_name);
					if (image_data == null) {
						System.out.println("Cannot find this image.");
					} else {
						FileOutputStream o = new FileOutputStream(file_name);
						o.write(image_data);
						o.close();
						System.out.println("Successfully Get the file from Bundle.\n");
					}
				} catch (IOException e) {
					System.out.println(e.toString());
				}
			}
		} else if (args[0].equals("delete")) {
			try {
				Path bundle_path = new Path(args[1]);
				String file_name = args[2];
				CloudImageBundle cib = new CloudImageBundle(bundle_path, conf);
				int change = cib.delImage(bundle_path, file_name, index);
				if (change == -1) {
					System.out.println("This file is not a Bundle file.\n");
				} else if (change == 0) {
					System.out.println("No found.\n");
				} else {
					// Delete the original index
					fs.delete(bundle_path, Boolean.TRUE);
					// Move the new index into original position
					String tmp_index = "/tmp-index";
					fs.rename(new Path(tmp_index), bundle_path);
					System.out.println("Succefully delete " + change + " image file.");
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		} else if (args[0].equals("update")) {
			try {
				Path bundle_path = new Path(args[1]);
				Path data_path = new Path(args[1] + "_dat");
				CloudImageBundle cib = new CloudImageBundle(bundle_path, conf);
				int change = cib.update(bundle_path, data_path, index);
				if (change == -1) {
					System.out.println("There's something wrong in update operation.");
				} else if (change == 0) {
					System.out.println("There's no change in the new bundle.");
				} else {
					fs.delete(bundle_path, Boolean.TRUE);
					fs.delete(data_path, Boolean.TRUE);

					String tmp_index = "/tmp-index";
					String tmp_data = "/tmp-data";
					fs.rename(new Path(tmp_index), bundle_path);
					fs.rename(new Path(tmp_data), data_path);

					System.out.println("Successfully decrease " + change + " image files.");
					changed = 1;
					index -= change;
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		} else if (args[0].equals("list")) {
			try {
				Path bundle_path = new Path(args[1]);
				System.out.println("Index = " + index);
				CloudImageBundle cib = new CloudImageBundle(bundle_path, index, conf);
				cib.listImage();
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		} else {
			System.out.println("Sorry, no other operations now. Will update soon.\n");
		}

		// Store index into num file.
		if (changed == 1) {
			try {
				DataOutputStream dos = fs.create(path_num);
				dos.writeInt(index);
				dos.flush();
				dos.close();
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

	}

}
