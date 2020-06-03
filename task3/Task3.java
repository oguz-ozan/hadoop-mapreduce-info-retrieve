import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.tools.jstat.Token;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Task3 {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {


		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] preWords = line.split("\t");

			int num = 0;

			// outputKey --> dosya ismi
			// prewords[i+1] -->  number (n)
			// newLine[0] -->  kelime
			// newLine[1] --> dosya ismi

			for(int i=0;i<preWords.length;i+=2){
				String[] words = preWords[i+1].split(" ");
 				Text outputKey = new Text(words[0]);
 				Text outputValue = new Text(preWords[i] + " " + words[1] + " " + words[2]);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values,
						   Context context) throws IOException, InterruptedException {
			List<String> words = new ArrayList<String>();
			List<String> numbers = new ArrayList<String>();
			List<String> bigNumbers = new ArrayList<String>();

			int sum = 0;
			int ctr = 0;

			for (Text val : values) {
				ctr++;
				sum+= Integer.parseInt(val.toString().split(" ")[1]);
				numbers.add(val.toString().split(" ")[1]);
				bigNumbers.add(val.toString().split(" ")[2]);
				words.add(val.toString().split(" ")[0]);
			}

			for(int i=0;i<ctr;i++){
				context.write(key, new Text(words.get(i) + " " + numbers.get(i) + " " + bigNumbers.get(i) + " " + Integer.toString(sum)));
			}
//			for(int i=0;i<target.size();i++){
//				//context.write(key, new Text(target.get(i).toString().split(" ")[0]));
//
//			}



//			for (Text val : values) {
//				Text txt = new Text(val.toString().split(" ")[1] + " " + key + val.toString().split(" ")[0]);
//				result.set(sum);
//				context.write(txt, new Text("" + result));
//			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count3");
		job.setJar("Task3.jar");
		job.setJarByClass(Task3.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

