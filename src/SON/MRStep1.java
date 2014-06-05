package SON;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.ToolRunner;

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
	
	//Opzionale:set the combiner class
	if(conf.getBoolean("useCombiner", false))
		job.setCombinerClass(MRStep1Combiner.class);
	
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
	  
	if (args.length < 4) {
      System.out.println("Usage: SON <num_reducers> <support_threshold> <input_path> <output_path> [optionName=optionValue]* ");
      System.exit(0);
    }
	
	int numReducers = Integer.parseInt(args[0]);
	double supportThreshold = Double.parseDouble(args[1]);
	Path inputPath = new Path(args[2]);
	Path outputDir = new Path(args[3]);
	Path tmpOutputDir = new Path(outputDir.toString()+"_tmp"); 
	
	
	Configuration conf = new Configuration();
	//conf.set("mapred.map.child.java.opts", "-Xmx512m");
	conf.setDouble("s", supportThreshold);
	//conf.set("dfs.blocksize","67108864");
	//conf.setInt("mapreduce.job.maps",3); 
	conf.setLong("mapreduce.task.timeout",3600000*3);
	
	//gestisto gli argomenti non obbligatori
	for(int i=4; i < args.length; i++){
		if(args[i].toLowerCase().contains("sizeofchunk"))
			conf.setInt("sizeOfChunk", Integer.parseInt(args[i].split("=")[1]));  
		else if(args[i].toLowerCase().contains("usecombiner"))
			conf.setBoolean("useCombiner", Boolean.parseBoolean(args[i].split("=")[1]));
		else if(args[i].toLowerCase().contains("usecomparator"))
			conf.setBoolean("useComparator", Boolean.parseBoolean(args[i].split("=")[1]));
		
	}
	
    int nRecord = ToolRunner.run(conf, new MRStep1(numReducers, inputPath, tmpOutputDir), args);
    conf.setInt("basketReaded", nRecord);
    
    int res = ToolRunner.run(conf, new MRStep2(numReducers, inputPath, outputDir), args);
    
    
    //elimino i risultati intermedi
    FileSystem fs = FileSystem.get(conf); 
    fs.delete(tmpOutputDir, true); 
    
    System.exit(res);
  }
}

class MRStep1Mapper extends Mapper<LongWritable,
									Text,
									Itemset,
									IntWritable> {

	Vector<File> chunks;
	int nFile;
	int sizeOfChunk;
	int currentSize;
	
	BufferedWriter bw;
	
	
	@Override
  	protected void setup(Context context) throws IOException{
		nFile = 0;
		currentSize = 0;
		chunks = new Vector<File>(); 
		sizeOfChunk = context.getConfiguration().getInt("sizeOfChunk", -1);
		System.out.println("sizeOfChunk="+sizeOfChunk);
		
		chunks.add(new File("Temp"+this+"_"+nFile+".txt"));
		bw = new BufferedWriter(new FileWriter(chunks.get(nFile)));

  	}
	
	@Override
  	protected void map( LongWritable key,
						  Text value, 
						  Context context) throws IOException, InterruptedException {
		
		
		String line = value.toString();
		
		if(sizeOfChunk != -1  && currentSize > sizeOfChunk){
			
			bw.close();
			
			nFile++;
			currentSize =0;
			chunks.add(new File("Temp"+this+"_"+nFile+".txt"));
			bw = new BufferedWriter(new FileWriter(chunks.get(nFile)));
		}
		
		bw.write(line+"\n");
		currentSize++;
  	}
	

  
  	@Override
  	protected void cleanup(Context context) throws IOException, InterruptedException {
  		bw.close();
  		

  		double s = context.getConfiguration().getDouble("s", 100);
  				
  		for(File f: chunks){
  			System.out.println("nuovo file: "+f.toString());
  			GregorianCalendar inizio = new GregorianCalendar();
  			
	  		MRApriori a = new MRApriori(f, s, context);
	  		a.start();
	  		
	  		GregorianCalendar fine = new GregorianCalendar();
	  		System.out.println("tempo exec: "+(fine.getTimeInMillis()-inizio.getTimeInMillis())/1000 +" sec");
			
			
			f.delete();
  		}
		
	}
}

class MRStep1Reducer extends Reducer<Itemset,
									IntWritable,
									Itemset,
									IntWritable> {
	
	private IntWritable one = new IntWritable(1);
	

	@Override
	protected void reduce(Itemset key,
							Iterable<IntWritable> values,
							Context context) throws IOException, InterruptedException {
		
		context.write(key, one);
	
	}
}


class MRStep1Combiner extends Reducer<Itemset,
										IntWritable,
										Itemset,
										IntWritable> {
	
	private IntWritable one = new IntWritable(1);
	
	protected void setup(Context context) throws IOException{
		System.out.println("creato combiner");
	}
	
	
	@Override
	protected void reduce(Itemset key, 
							Iterable<IntWritable> values, 
							Context context) throws IOException, InterruptedException {

		context.write(key, one);
	
	}
}
