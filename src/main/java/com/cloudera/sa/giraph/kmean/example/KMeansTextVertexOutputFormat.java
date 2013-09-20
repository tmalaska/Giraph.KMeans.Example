package com.cloudera.sa.giraph.kmean.example;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KMeansTextVertexOutputFormat extends TextVertexOutputFormat<Text, Text, Text> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new TreeRecordTextWriter();
	}

	public class TreeRecordTextWriter extends TextVertexWriter {

		Text newKey = new Text();
		Text newValue = new Text();
		
		public void writeVertex(
				Vertex<Text, Text, Text, ?> vertex)
				throws IOException, InterruptedException {
			
			
			newKey.set(vertex.getId().toString() + "," + vertex.getValue());
			
			getRecordWriter().write(newKey, newValue);
			
		}
		
	}
}
