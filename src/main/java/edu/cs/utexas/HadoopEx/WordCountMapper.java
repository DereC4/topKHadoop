package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, List<String>, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		// StringTokenizer itr = new StringTokenizer(value.toString());
		String[] temp = value.toString().split(",");
		// // while (itr.hasMoreTokens()) {
		// // 	word.set(itr.nextToken());
		// // 	context.write(word, counter);
		// // }
		// String airport = temp[7];
		// word.set(airport);
		// context.write(word, counter);
		String airport = temp[7];
		String delay = temp[11];
		// StringBuilder temp3 = new StringBuilder();
		// temp2.append(airport);
		// temp2.append(",");
		// temp2.append(delay);
		List<String> temp2 = new ArrayList<>();
		temp2.add(airport);
		temp2.add(delay);
		// word.set(temp2);
		context.write(temp2, counter);
		
	}
}