package SON;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.Vector;

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

public class MRStep1 extends Configured implements Tool {

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
	job.setMapperClass(MRStep1Mapper.class);
	job.setMapOutputKeyClass(Itemset.class);
	job.setMapOutputValueClass(IntWritable.class);
	//set reduce class and the reduce output key and value classes
	job.setReducerClass(MRStep1Reducer.class);
	job.setOutputKeyClass(Itemset.class);
	job.setOutputValueClass(IntWritable.class);
	//set job output format
	job.setOutputFormatClass(TextOutputFormat.class);
	
//	job.setSortComparatorClass(ItemsetComparator.class);
	
	//add the input file as job input (from HDFS)
	FileInputFormat.addInputPath(job, inputPath);
	//set the output path for the job results (to HDFS)
	FileOutputFormat.setOutputPath(job, outputDir);
	//set the number of reducers. This is optional and by default is 1
	job.setNumReduceTasks(numReducers);
	
	
	//set the jar class
	job.setJarByClass(getClass());
	
	//run the first job
	job.waitForCompletion(true); //? 0 : 1; // this will execute the job
	
	//get the number of records and store it in a configuration variable
	Long nRecord = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
	//conf.setLong("basketReaded", nRecord);
	return nRecord.intValue();
	
	//return res ? 0 : 1;
  }

  
  public MRStep1(int numReducers, Path inputPath, Path outputDir){
	  this.numReducers = numReducers; 
	  this.inputPath = inputPath;
	  this.outputDir = outputDir;
  }
  
  
  
  public static void main(String args[]) throws Exception {
	if (args.length != 4) {
      System.out.println("Usage: SON <num_reducers> <support_threshold> <input_path> <output_path>");
      System.exit(0);
    }
	
	int numReducers = Integer.parseInt(args[0]);
	int supportThreshold = Integer.parseInt(args[1]);
	Path inputPath = new Path(args[2]);
	Path outputDir = new Path(args[3]);
	Path tmpOutputDir = new Path(outputDir.toString()+"_tmp"); 
	
	
	Configuration conf = new Configuration();
	conf.set("mapred.map.child.java.opts", "-Xmx512m");
	conf.setInt("s", supportThreshold);
	  
	
	
    int nRecord = ToolRunner.run(conf, new MRStep1(numReducers, inputPath, tmpOutputDir), args);
    conf.setInt("basketReaded", nRecord);
    
    int res = ToolRunner.run(conf, new MRStep2(numReducers, inputPath, outputDir), args);
    
    System.exit(res);
  }
}

class MRStep1Mapper extends Mapper<LongWritable, //input key type //è l offset del file testo
									Text, //input value type //è la riga i-esima del file
									Itemset, //output key type
									IntWritable> {//change Object to output value type

	private Vector<Vector<Integer>> baskets;
	
	@Override
  	protected void setup(Context context){
		baskets = new Vector<Vector<Integer>>();
  	}
	
	@Override
  	protected void map( LongWritable key, //input key type
						  Text value, //input value type
						  Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		baskets.add(createBasket(line));
		
  	}
	
	public static Vector<Integer> createBasket(String text){
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
  		System.out.println("soglia map="+context.getConfiguration().getInt("s", 100));
  		Apriori a = new Apriori(baskets,context.getConfiguration().getInt("s", 100));
		a.start();
		
		Itemset app = new Itemset();
		IntWritable one = new IntWritable(1);
		
		for(Vector<Integer> itemset: a.getCandidateItemset()){
			app.set(itemset);
			context.write(app, one);
		}
		
	}
}

class MRStep1Reducer extends Reducer<Itemset,
									IntWritable,
									Itemset,
									IntWritable> {
	
	private IntWritable one = new IntWritable(1);
	

	@Override
	protected void reduce(Itemset key, //input key type
							Iterable<IntWritable> values, //input value type
							Context context) throws IOException, InterruptedException {
		
		context.write(key, one);
	
	}
}
