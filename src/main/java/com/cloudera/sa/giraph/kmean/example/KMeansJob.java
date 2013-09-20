package com.cloudera.sa.giraph.kmean.example;

import java.io.IOException;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * Hello world!
 *
 */
public class KMeansJob 
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	if (args.length != 6) {
			System.out.println("KMeansJob Help:");
			System.out.println("Parameters: KMeansJob <numbersOfWorkers> <numberOfClusters> <numberOfDimensions> <maxIterations> <inputLocaiton> <outputLocation>");
			System.out.println("KMeansJob 2 2 2 2 inputFolder outputFolder");
			return;
		}
		
		
		String numberOfWorkers = args[0];
		String numberOfClusters = args[1];
		String numberOfDimensions = args[2];
		String maxIterations = args[3];
		String inputLocation = args[4];
		String outputLocation = args[5];
		
	    GiraphJob bspJob = new GiraphJob(new Configuration(), KMeansJob.class.getName());
	    
	    bspJob.getConfiguration().set(Const.NUMBER_OF_CLUSTERS, numberOfClusters);
	    bspJob.getConfiguration().set(Const.NUMBER_OF_DIMENSIONS, numberOfDimensions);
	    bspJob.getConfiguration().set(Const.MAX_ITERATIONS, maxIterations);
	    
	    bspJob.getConfiguration().setVertexClass(KMeansNodeVertix.class);
	    bspJob.getConfiguration().setVertexInputFormatClass(KMeansTextVertexInputFormat.class);
	    GiraphFileInputFormat.addVertexInputPath(bspJob.getConfiguration(), new Path(inputLocation));
	    
	    bspJob.getConfiguration().setVertexOutputFormatClass(KMeansTextVertexOutputFormat.class);
	    bspJob.getConfiguration().setWorkerContextClass(KMeansNodeWorkerContext.class);
	    bspJob.getConfiguration().setMasterComputeClass(KMeansVertixMasterCompute.class);
	    
	    int minWorkers = Integer.parseInt(numberOfWorkers);
	    int maxWorkers = Integer.parseInt(numberOfWorkers);
	    bspJob.getConfiguration().setWorkerConfiguration(minWorkers, maxWorkers, 100.0f);

	    FileOutputFormat.setOutputPath(bspJob.getInternalJob(),
	                                   new Path(outputLocation));
	    boolean verbose = true;
	    
	    Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
        
        hdfs.delete(new Path(outputLocation), true);
	    
	    if (bspJob.run(verbose)) {
	      System.out.println("Ended Good");
	    } else {
	      System.out.println("Ended with Failure");
	    }
    }
}
