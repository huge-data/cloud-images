
## 基于HDFS的分布式图像处理框架HI

> 开发环境在Hadoop 2.2.0。


### 使用说明

1. 安装Hadoop运行环境

  a. Download Hadoop 2.2.0 from offcial website: http://hadoop.apache.org/
  b. Setup Hadoop's configuration files as needed: core-site.xml, 
     hdfs-site.xml, slaves etc.
  c. Copy Hadoop installation to each node. 
  d. Start Hadoop service.
  
  More details can be seen in Hadoop official homepage:
  http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html

  Note: Setting more memory in mapred-site.xml will better support HI when the
  data size is really large.

2. 编译

  Enter into HI's root directory, and use command "mvn clean install" to compile HI.

3. HI配置

  In the bin/ directory, there are two important parts, cloudip script and 
  operation configuration XML files.

  Cloudip:
      The cloudip script is the main file to start the Hadoop jobs for HI. 
  
  XML configuration file:
       The XML configuration file allows the user to define inputs, chain, and ouputs
       for HI. Cloudip reads the necessary information in the XML file and then 
       starts the convolution of Hadoop image processing jobs.

  The XML file needs to be written in the XML format. In the convolution file, there
  are three parts: Inputs, Outputs and Kernels. The tags of the three parts are
  <inputs>, <outputs> and <chain>. Each <inputs>, <outputs> and <chain> can include
  multiple <input>, <output> and <operation> tags.

  <input> - Defines where the images are retrieved from the local filesystem and the
             location of the data to store in HDFS.

	<src>  - Field defines where on the local filesystem to retrieve images
    <des>  - Field defines where on HDFS to store images. The <des> field requires 
             the path to have the prefix "hdfs://".  


  <output> - Defines where the resulting images will be retrieved from in HDFS and the
              location of the data to store on the local filesystem.  

	<src>  - Field defines where on HDFS to retrieve images. The <des> field requires 
             the path to have the prefix "hdfs://".  
    <des>  - Field defines where on the local filesystem to store the images
            
    
  <operation> - For each specific image processing operation, the start and end tags are defined as 
             <operation> and </operation>. Users will need to define the following fields: 
 
    <name> - Name of the operation
   <input> - Location of the input files (relative to HDFS)
  <output> - Location to store output files (relative to HDFS)
  <kernel> - Kernel file only for convolution operation (CON)
    <type> - Computation type only for convolution operation


 当前版本支持的图像处理算法如下：

  Name		Operation
  -------------------------------------------------------------------------------
  HE		Histogram Equalization
  GF		Gaussian Filter
  CON		Convolution
  IG		Image Gradient
  FFT		2D Fastest Fourier Transform
 
示例:

  <inputs>
	<input>
		<src>/local-dir</src>
		<des>hdfs://hdfs-dir</des>
	</input>
  </inputs>

  <chain>
	<operation> 
		<name>HE</name> 
		<input>hdfs://input-example</input>
		<output>/output-example1</output>
	</operation> 

	<operation> 
		<name>GF</name> 
		<input>/output-example1</input>
		<output>/output-example2</output>
	</operation> 
	<operation> 
		<name>CON</name> 
		<input>hdfs://otuput-example2</input>
		<output>/output-example3</output>
		<kernel>kernel_file</kernel>
		<type>0</type>
	</operation> 
  </chain>

  <outputs>
  	<output>
		<src>hdfs://hdfs-dir</src>
		<des>/local-dir</des>
	</output>
  </outputs>

  The bin/ directory contains two examples of configuration files:
  example.xml and example.xml.template. 

  For the convolution opertaion, the user need provide a kernel file and 
  kernel's computation type (0 or 1). In this kernel file, the first two 
  lines show the kernel's width and height. The other lines show the detailed 
  values of this kernel. We provide an example kernel file (GF_kernel) in 
  bin directory.

4. 运行HI

  Run cloudip with a XML file in bin directory.
  For example:
  ./cloudip example.xml

  After a chain of operations is finished, users can find log file (log_file)
  in the bin directory. In this log file, users can get information regarding
  the runing time and each operation's running condition.

  Please also note that the result will remain in HDFS until you remove them. So 
  make sure the src & des directory is available before you run the job.
