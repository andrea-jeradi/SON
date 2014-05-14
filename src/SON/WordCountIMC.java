package SON;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import FrequentItemset.Apriori;

import java.util.Collections;
import java.util.StringTokenizer;
import java.util.Vector;
/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 * 
 */
public class WordCountIMC extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {
	  
	Configuration conf = this.getConf();
			
	//define new job instead of null using conf e setting a name
	Job job = new Job(conf, "SON_step1");
	//set job input format
	job.setInputFormatClass(TextInputFormat.class);
	//set map class and the map output key and value classes
	job.setMapperClass(WCIMCMapper.class);
	job.setMapOutputKeyClass(Itemset.class);
	job.setMapOutputValueClass(IntWritable.class);
	//set reduce class and the reduce output key and value classes
	job.setReducerClass(WCIMCReducer.class);
	job.setOutputKeyClass(Itemset.class);
	job.setOutputValueClass(IntWritable.class);
	//set job output format
	job.setOutputFormatClass(TextOutputFormat.class);
	//add the input file as job input (from HDFS)
	FileInputFormat.addInputPath(job, inputPath);
	//set the output path for the job results (to HDFS)
	FileOutputFormat.setOutputPath(job, outputDir);
	//set the number of reducers. This is optional and by default is 1
	job.setNumReduceTasks(numReducers);
	//set the jar class
	job.setJarByClass(getClass());
	
	return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
  }

  public WordCountIMC (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
    System.exit(res);
  }
}

class WCIMCMapper extends Mapper<LongWritable, //input key type //è l offset del file testo
									Text, //input value type //è la riga i-esima del file
									Itemset, //output key type
									IntWritable> {//change Object to output value type

	private Vector<Vector<Integer>> baskets;
	
	@Override
  	protected void setup(Context context){
		baskets = new Vector<Vector<Integer>>();
  	}
	
	@Override
  	protected void map(	LongWritable key, //input key type
							Text value, //input value type
							Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		baskets.add(createBasket(line));
		
  	}
	
	private Vector<Integer> createBasket(String text){
		StringTokenizer st = null;
		Vector<Integer> items = new Vector<Integer>();
		
		st = new StringTokenizer(text); // Tokenizzo il basket.
			
		while(st.hasMoreTokens()) {
			items.add(Integer.parseInt(st.nextToken())); // Aggiungo ogni item al vettore.
		}
		
		Collections.sort(items);
		return items;
	}
	
  	
  
  	@Override
  	protected void cleanup(Context context) throws IOException, InterruptedException {
  		Apriori a= new Apriori(baskets,40);
		a.start();
		
		Itemset app = new Itemset();
		IntWritable one = new IntWritable(1);
		
		for(Vector<Integer> itemset: a.getCandidateItemset()){
			app.set(itemset);
			context.write(app, one);
		}
		
	}
}

class WCIMCReducer extends Reducer<Itemset, //input key type
									IntWritable, //input value type
									Itemset, //output key type
									IntWritable> { //output value type
	
	private IntWritable one = new IntWritable(1);
	

	@Override
	protected void reduce(Itemset key, //input key type
							Iterable<IntWritable> values, //input value type
							Context context) throws IOException, InterruptedException {
						
		context.write(key, one);
	
	}
}
