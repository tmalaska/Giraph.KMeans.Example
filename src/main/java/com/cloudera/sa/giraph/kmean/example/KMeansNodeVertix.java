package com.cloudera.sa.giraph.kmean.example;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class KMeansNodeVertix extends
Vertex<Text, Text, Text, Text>{

	Pattern commonSpliter = Pattern.compile(",");
	
	@Override
	public void compute(Iterable<Text> messages) throws IOException {
		
		KMeansNodeWorkerContext workerContext = (KMeansNodeWorkerContext) getWorkerContext();
		
		long superstep = getSuperstep();
		int numberOfDim = workerContext.getNumberOfDimensions();
		
		//This is bad I should store the values more native
		String origValue = getValue().toString();
		
		String[] pointsStrings = commonSpliter.split(origValue);
		
		if (superstep >= workerContext.getMaxIterations()) {
			voteToHalt();
		} else if (superstep == 0) {
			//Here we will determine the globe maxs and mins
			double[] points = new double[pointsStrings.length];
			for (int i = 0; i < pointsStrings.length; i++) {
				points[i] = Double.parseDouble(pointsStrings[i]);
			}
			
			for (int i = 0; i < numberOfDim; i++) {
				aggregate(Const.MAX_DIMENSION_PREFIX + "." + i, new DoubleWritable(points[i]));
				aggregate(Const.MIN_DIMENSION_PREFIX + "." + i, new DoubleWritable(points[i]));
			}
		} else {
			double[] points = parsePointsFromValue(superstep, pointsStrings);
			int clusterCenter = selectClusterCenter(workerContext, points);
			applyClusterCenterAggregates(clusterCenter, points);
			updateValue(superstep, origValue, clusterCenter);
		}
	}

	private double[] parsePointsFromValue(long superstep, String[] pointsStrings) {
		double[] points = new double[pointsStrings.length - (superstep == 1?0:1)];
		for (int i = 0; i < points.length; i++) {
			points[i] = Double.parseDouble(pointsStrings[i]);
		}
		return points;
	}

	private void updateValue(long superstep, String origValue, int clusterCenter) {
		//This can be made faster, string are very slow
		if (superstep == 1) {
			this.setValue(new Text(origValue + "," + clusterCenter));
		} else {
			this.setValue(new Text(origValue.substring(0, origValue.lastIndexOf(',')) + "," + clusterCenter));
		}
	}
	
	private int selectClusterCenter(KMeansNodeWorkerContext workerContext, double points[]) {
		double[][] centerPoints = workerContext.getCenters();
		
		int selectedCluster = -1;
		double shortestDistance = Double.MAX_VALUE;
		
		for (int c = 0; c < centerPoints.length; c++) {
			double distance = 0;
			
			for (int d = 0; d < centerPoints[c].length; d++) {
				//This can be made faster
				double dimDistance = Math.abs(centerPoints[c][d] - points[d]);
				distance = Math.sqrt(Math.pow(distance, 2) + Math.pow(dimDistance, 2));
			}
			
			if (distance < shortestDistance) {
				selectedCluster = c;
				shortestDistance = distance;
			}
		}
		return selectedCluster;
	}
	
	private void applyClusterCenterAggregates(int clusterCenter, double[] points) {
		for (int d = 0; d < points.length; d++) {
			aggregate(Const.MAX_DIMENSION_PREFIX + "." + clusterCenter + "." + d, new DoubleWritable(points[d]));
			aggregate(Const.MIN_DIMENSION_PREFIX + "." + clusterCenter + "." + d, new DoubleWritable(points[d]));
		}
		aggregate(Const.CLUSTER_NODE_COUNT_PREFIX + "." + clusterCenter, new LongWritable(1));
	}
}
