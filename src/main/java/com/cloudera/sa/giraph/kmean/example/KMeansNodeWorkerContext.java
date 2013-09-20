package com.cloudera.sa.giraph.kmean.example;

import java.util.regex.Pattern;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class KMeansNodeWorkerContext extends WorkerContext {

	
	private double[][] centers;
    
	private int numberOfClusters;
	private int numberOfDimensions;
	private int maxIterations;
	
	
	private final static Pattern commaPattern = Pattern.compile(",");
	
	
	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
		numberOfClusters = Integer.parseInt(getContext().getConfiguration().get(Const.NUMBER_OF_CLUSTERS));
		numberOfDimensions = Integer.parseInt(getContext().getConfiguration().get(Const.NUMBER_OF_DIMENSIONS));
		
		//System.out.println("numberOfClusters:" + numberOfClusters);
		//System.out.println("numberOfDimensions:" + numberOfDimensions);
		centers = new double[numberOfClusters][numberOfDimensions];
		
		maxIterations = Integer.parseInt(getContext().getConfiguration().get(Const.MAX_ITERATIONS));
	}

	@Override
	public void postApplication() {
		
	}

	@Override
	public void preSuperstep() {

		Text pointsText = ((Text)getAggregatedValue(Const.CENTER_POINTS));
		
		if (pointsText != null) {
			String pointsString = pointsText.toString();
			if (!pointsString.isEmpty()) {
				String[] points = commaPattern.split(pointsString);
				int pointIndex = 0;
				for (int c = 0; c < numberOfClusters; c++) {
					for (int d = 0; d < numberOfDimensions; d++) {
						
						//System.out.println("centers[" + c + "][" + d + "]=" + points[pointIndex]);
						
						centers[c][d] = Double.parseDouble(points[pointIndex]);
						pointIndex++;
					}
				}
			}
		}
		
	}

	@Override
	public void postSuperstep() {
	}

	public double[][] getCenters() {
		return centers;
	}

	public int getNumberOfClusters() {
		return numberOfClusters;
	}

	public int getNumberOfDimensions() {
		return numberOfDimensions;
	}
	
	public int getMaxIterations() {
		return maxIterations;
	}

	
	
}
