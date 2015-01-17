CloudImageProcessing Project
===========

The CloudImageProcessing (CIP) is an image processing framework based on Hadoop.
CIP is designed and implemented by the Parallel Architecture & System Laboratory
(PASL) at Auburn University.

This version of CIP is based on Hadoop 2.2.0. You can use it without any major 
changes to Hadoop.


Directory Structure
===========

CloudImageProcessing
|-- pom.xml						
|-- ReadME 						
|-- bin							
|	|-- cloudip	
|	|-- example.xml				
|	`-- example.xml.template				
|-- cloudimage-bundle			
|	|-- src						
|	|-- target					
|	`-- pom.xml					
|-- cloudimage-codec			
|	|-- src						
|	|-- target                  
|	`-- pom.xml                 
|-- cloudimage-imageprocessing
|	|-- src						
|	|-- target                  
|	`-- pom.xml                 
`-- cloudimage-mapreduce
	|-- src						
	|-- target                  
	`-- pom.xml                 


Instructions
===========

1. Setup Hadoop running environment.

  a. Download Hadoop 2.2.0 from offcial website: http://hadoop.apache.org/
  b. Setup Hadoop's configuration files as needed: core-site.xml, 
     hdfs-site.xml, slaves etc.
  c. Copy Hadoop installation to each node. 
  d. Start Hadoop service.
  
  More details can be seen in Hadoop official homepage:
  http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html

  Note: Setting more memory in mapred-site.xml will better support CIP when the
  data size is really large.

2. Compile CIP.

  Enter into CIP's root directory, and use command "mvn clean install" to compile CIP.

3. CIP Configuration

  In the bin/ directory, there are two important parts, cloudip script and 
  operation configuration XML files.

  Cloudip:
      The cloudip script is the main file to start the Hadoop jobs for CIP. 
  
  XML configuration file:
       The XML configuration file allows the user to define inputs, chain, and ouputs
       for CIP. Cloudip reads the necessary information in the XML file and then 
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


 Currently support image processing algorithms.

  Name		Operation
  -------------------------------------------------------------------------------
  HE		Histogram Equalization
  GF		Gaussian Filter
  CON		Convolution
  IG		Image Gradient
  FFT		2D Fastest Fourier Transform
 
Example:

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

4. Run CIP

  Run cloudip with a XML file in bin directory.
  For example:
  ./cloudip example.xml

  After a chain of operations is finished, users can find log file (log_file)
  in the bin directory. In this log file, users can get information regarding
  the runing time and each operation's running condition.

  Please also note that the result will remain in HDFS until you remove them. So 
  make sure the src & des directory is available before you run the job.
