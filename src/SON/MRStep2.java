package SON;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class MRStep2 extends Configured implements Tool {

	private int numReducers;
	private Path inputPath;
	private Path outputDir;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		// define new job instead of null using conf e setting a name
		Job job = new Job(conf, "SON_step2");
		// set job input format
		job.setInputFormatClass(TextInputFormat.class);
		// set map class and the map output key and value classes
		job.setMapperClass(MRStep2Mapper.class);
		job.setMapOutputKeyClass(Itemset.class);
		job.setMapOutputValueClass(IntWritable.class);
		// set reduce class and the reduce output key and value classes
		job.setReducerClass(MRStep2Reducer.class);
		job.setOutputKeyClass(Itemset.class);
		job.setOutputValueClass(IntWritable.class);
		// set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		if (conf.getBoolean("useComparator", true))
			job.setSortComparatorClass(ItemsetComparator.class);

		// add the input file as job input (from HDFS)
		FileInputFormat.addInputPath(job, inputPath);
		// set the output path for the job results (to HDFS)
		FileOutputFormat.setOutputPath(job, outputDir);
		// set the number of reducers. This is optional and by default is 1
		job.setNumReduceTasks(numReducers);
		// set the jar class
		job.setJarByClass(getClass());

		return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
	}

	public MRStep2(int numReducers, Path inputPath, Path outputDir) {
		this.numReducers = numReducers;
		this.inputPath = inputPath;
		this.outputDir = outputDir;
	}

}

// class MRStep2Combiner extends Reducer<Itemset,
// IntWritable,
// Itemset,
// IntWritable> {
//
// IntWritable count = new IntWritable();
//
// @Override
// protected void reduce(Itemset key,
// Iterable<IntWritable> values,
// Context context) throws IOException, InterruptedException {
//
// int sum=0;
// for(IntWritable i:values)
// sum += i.get();
//
// count.set(sum);
// context.write(key, count);
//
// }
// }

