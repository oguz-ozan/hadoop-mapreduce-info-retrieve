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

public class Task1 {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			/*StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}*/

			List<String> stopWords = new ArrayList<String>();
			stopWords = Files.readAllLines(Paths.get("/Users/oguzozan/Downloads/tr-stopword-list.txt"),StandardCharsets.ISO_8859_1);

			String line = value.toString().replaceAll("\\p{P}", "").toLowerCase();
			String[] preWords = line.split("\t");
			String newLine = "";
			for(int i=0;i<preWords.length;i++){
				if(i%3==0){
					newLine = newLine + preWords[i];
				}
			}
			String[] words = newLine.split(" ");

			for (String word : words) {
				boolean isHttp = false;
				if(word.length() > 2 && !Character.isDigit(word.charAt(0))){
					if(!stopWords.contains(word)){
						if(word.length() > 5) {
							isHttp = true;
						}
						if(!isHttp){
							String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
							String doc = word + " " + fileName;
							Text outputKey = new Text(doc);
							IntWritable outputValue = new IntWritable(1);
							context.write(outputKey, outputValue);
						}
					}
				}



			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");
		job.setJar("Task1.jar");
		job.setJarByClass(Task1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
