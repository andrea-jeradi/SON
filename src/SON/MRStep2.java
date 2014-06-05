package SON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class MRStep2 extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {
	  
	Configuration conf = this.getConf();
			
	//define new job instead of null using conf e setting a name
	Job job = new Job(conf, "SON_step2");
	//set job input format
	job.setInputFormatClass(TextInputFormat.class);
	//set map class and the map output key and value classes
	job.setMapperClass(MRStep2Mapper.class);
	job.setMapOutputKeyClass(Itemset.class);
	job.setMapOutputValueClass(IntWritable.class);
	//set reduce class and the reduce output key and value classes
	job.setReducerClass(MRStep2Reducer.class);
	job.setOutputKeyClass(Itemset.class);
	job.setOutputValueClass(IntWritable.class);
	//set job output format
	job.setOutputFormatClass(TextOutputFormat.class);
	
	//Opzionale:set the combiner class
	if(conf.getBoolean("useCombiner", false))
		job.setCombinerClass(MRStep2Combiner.class);
	
	if(conf.getBoolean("useComparator", true))
		job.setSortComparatorClass(ItemsetComparator.class);
	
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

  public MRStep2(int numReducers, Path inputPath, Path outputDir){
	  this.numReducers = numReducers; 
	  this.inputPath = inputPath;
	  this.outputDir = outputDir;
  }

}

class MRStep2Mapper extends Mapper<LongWritable, 
									Text,
									Itemset,
									IntWritable> {

	HashMap<Vector<Integer>,Integer> candidateItemset = new HashMap<Vector<Integer>,Integer>();
	
	
	@Override
  	protected void setup(Context context){
		
		Path path;
		FileSystem fs;
		FSDataInputStream  file;
		BufferedReader input;
		FileStatus[] status;
		String text;
		
		try {
			System.out.println(context.getWorkingDirectory().toString());
			System.out.println(FileOutputFormat.getOutputPath(context).toString());
					
			
			path = new Path(FileOutputFormat.getOutputPath(context).toString()+"_tmp");
			fs = FileSystem.get(path.toUri(),context.getConfiguration());
			status = fs.listStatus(path);
		
			for(FileStatus st : status){
				if(st.isFile() && !st.getPath().toString().contains("_SUCCESS")){
					//System.out.println("file dir: "+st.getPath());
					
					
					file = fs.open(st.getPath());
					input = new BufferedReader(new InputStreamReader(file));
					
						while ((text = input.readLine()) != null) {
							text= text.substring(0, text.length()-2).trim();
							candidateItemset.put(createBasket(text), 0);
						}
				}
			}
								
		} catch (IOException e) {
			e.printStackTrace();
		}

		
  	}
	
	@Override
  	protected void map( LongWritable key,
						  Text value,
						  Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		Vector<Integer> basket = createBasket(line); 
		boolean find;
		
		for(Vector<Integer> itemset :candidateItemset.keySet()){
			find = true;
			for(int item: itemset){
				if(! basket.contains(item)){
					find = false;
					break;
				}
			}
			if(find){
				candidateItemset.put(itemset, candidateItemset.get(itemset) + 1);
			}
		}
		
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
  		Itemset app = new Itemset();
  		IntWritable count = new IntWritable();
  		int temp;
  		
  		for(Vector<Integer> itemset :candidateItemset.keySet()){
  			temp = candidateItemset.get(itemset);
  			if(temp > 0){
				app.set(itemset);
				count.set(temp);
				context.write(app, count);
			}
		}
  		
	}
}

class MRStep2Reducer extends Reducer<Itemset,
									IntWritable,
									Itemset,
									IntWritable> { 
	
	double frequent;
	IntWritable count = new IntWritable();
	
	@Override
  	protected void setup(Context context){
		long basketReaded = context.getConfiguration().getInt("basketReaded", Integer.MAX_VALUE);
		double s = context.getConfiguration().getDouble("s", 100);
		
		frequent = basketReaded*s/100.0;
	}

	@Override
	protected void reduce(Itemset key,
							Iterable<IntWritable> values, 
							Context context) throws IOException, InterruptedException {
		
		int sum=0;
		  for(IntWritable i:values)
			  sum += i.get();
		  
		  if(sum >= frequent){
			  count.set(sum);
			  context.write(key, count);
		  }
		
	
	}
}


class MRStep2Combiner extends Reducer<Itemset,
									IntWritable,
									Itemset,
									IntWritable> { 
	
	IntWritable count = new IntWritable();	
	
	@Override
	protected void reduce(Itemset key,
							Iterable<IntWritable> values, 
							Context context) throws IOException, InterruptedException {
		
		int sum=0;
		for(IntWritable i:values)
			sum += i.get();
		
		count.set(sum);
		context.write(key, count);
	
	}
}

