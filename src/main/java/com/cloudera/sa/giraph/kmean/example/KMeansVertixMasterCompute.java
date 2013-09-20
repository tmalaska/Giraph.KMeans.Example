package com.cloudera.sa.giraph.kmean.example;


import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class KMeansVertixMasterCompute extends DefaultMasterCompute {
	
	int numberOfDimensions;
	int numberOfClusters;
	
	@Override
    public void compute() {
		System.out.println("---SuperStep: " + this.getSuperstep());
		
		if (getSuperstep() == 0) {
			
		} else if (getSuperstep() == 1) {
			StringBuilder newClusterCenters = new StringBuilder();
			for (int c = 0; c < numberOfClusters; c++) {
				for (int d = 0; d < numberOfDimensions; d++) {
					double max = ((DoubleWritable)getAggregatedValue(Const.MAX_DIMENSION_PREFIX + "." + d)).get();
					double min = ((DoubleWritable)getAggregatedValue(Const.MIN_DIMENSION_PREFIX + "." + d)).get();
					
					double newPoint = ((Math.random() * (max - min)) + min);
					newClusterCenters.append( (c>0||d>0?",":"") + newPoint);
				}
			}
			
			System.out.println("CenterPoints:" + newClusterCenters);
			this.setAggregatedValue(Const.CENTER_POINTS, new Text(newClusterCenters.toString()));
		} else {
			for (int c = 0; c < numberOfClusters; c++) {
				System.out.print(" - Cluster:" + c + " MinAndMaxs");
				for (int d = 0; d < numberOfDimensions; d++) {
					if (d > 0) {
						System.out.print(",");	
					} 
					System.out.print("[" + getAggregatedValue(Const.MAX_DIMENSION_PREFIX + "." + c + "." + d) + "," + 
							getAggregatedValue(Const.MIN_DIMENSION_PREFIX + "." + c + "." + d) + "]");
				}
				System.out.println(" nodes:" + getAggregatedValue(Const.CLUSTER_NODE_COUNT_PREFIX + "." + c));
			}
			
			StringBuilder newClusterCenters = new StringBuilder();
			
			for (int c = 0; c < numberOfClusters; c++) {
				for (int d = 0; d < numberOfDimensions; d++) {
					double max = ((DoubleWritable)getAggregatedValue(Const.MAX_DIMENSION_PREFIX + "." + c + "." + d)).get();
					double min = ((DoubleWritable)getAggregatedValue(Const.MIN_DIMENSION_PREFIX + "." + c + "." + d)).get();
					newClusterCenters.append( (c>0||d>0?",":"") + ((max + min)/2));
				}
			}
			this.setAggregatedValue(Const.CENTER_POINTS, new Text(newClusterCenters.toString()));
			
		} 
		
		System.out.println("---");
		
		
    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
    	
    	numberOfDimensions = Integer.parseInt(this.getConf().get(Const.NUMBER_OF_DIMENSIONS));
    	
    	for (int i = 0; i < numberOfDimensions; i++) {
	    	registerAggregator(Const.MAX_DIMENSION_PREFIX + "." + i,
	    	          DoubleMaxAggregator.class);
	    	
	    	registerAggregator(Const.MIN_DIMENSION_PREFIX + "." + i,
	    			DoubleMinAggregator.class);
    	}
    	
    	numberOfClusters = Integer.parseInt(this.getConf().get(Const.NUMBER_OF_CLUSTERS));
    	
    	for (int c = 0; c < numberOfClusters; c++) {
    		registerAggregator(Const.CLUSTER_NODE_COUNT_PREFIX + "." + c,
	    	          LongSumAggregator.class);
    		
    		for (int d = 0; d < numberOfDimensions; d++) {
    	    	registerAggregator(Const.MAX_DIMENSION_PREFIX + "." + c + "." + d,
    	    	          DoubleMaxAggregator.class);
    	    	
    	    	registerAggregator(Const.MIN_DIMENSION_PREFIX + "." + c + "." + d,
    	    			DoubleMinAggregator.class);
        	}
    			
    	}
    	
    	registerAggregator(Const.CENTER_POINTS, TextAppendAggregator.class);
    	
    }
}
