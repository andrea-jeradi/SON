package SON;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
/**
 * Questa classe si occupa di lanciare i processi di MapReduce secondo l'algoritmo SON.
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class Launcher {
	
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


