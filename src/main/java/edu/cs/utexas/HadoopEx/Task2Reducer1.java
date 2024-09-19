package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer1 extends  Reducer<Text, IntWritable, Text, IntWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int totalFlights = 0;
       
       for (IntWritable value : values) {
        totalFlights += value.get();
       }
       
       context.write(text, new IntWritable(totalFlights));
   }
}