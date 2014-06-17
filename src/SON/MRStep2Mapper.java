package SON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import FrequentItemset.Utils;

public class MRStep2Mapper extends
		Mapper<LongWritable, Text, Itemset, IntWritable> {

	Vector<Vector<Integer>> candidateItemsets = new Vector<Vector<Integer>>();
	int counters[];
	boolean flags[];
	HashMap<Integer,Vector<Integer>> index = new HashMap<Integer,Vector<Integer>>();

	@Override
	protected void setup(Context context) {

		Path path;
		FileSystem fs;
		FSDataInputStream file;
		BufferedReader input;
		FileStatus[] status;
		String text;
		
		Vector<Integer> itemset;
		
		try {			

			path = new Path(FileOutputFormat.getOutputPath(context).toString()
					+ "_tmp");
			fs = FileSystem.get(path.toUri(), context.getConfiguration());
			status = fs.listStatus(path);
			
			for (FileStatus st : status) {
				if (st.isFile()
						&& !st.getPath().toString().contains("_SUCCESS")) {

					file = fs.open(st.getPath());
					input = new BufferedReader(new InputStreamReader(file));

					while ((text = input.readLine()) != null) {
						text = text.substring(0, text.length() - 2).trim();
						itemset = createBasket(text);
						candidateItemsets.add(itemset);
					}
				}
			}
			
			Utils.orderItemset(candidateItemsets);
			counters = new int[candidateItemsets.size()];
			flags = new boolean[candidateItemsets.size()];
			for(int i=0;i<candidateItemsets.size();i++){
				counters[i] = 0;
				
				for(int item : candidateItemsets.get(i)){
					if(!index.containsKey(item)){
						index.put(item, new Vector<Integer>());
					}
					index.get(item).add(i);		
				}
			}												

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		Vector<Integer> basket = createBasket(line);
//		boolean find;
		
		for(int i=0;i<flags.length;i++)
			flags[i] = true;
		
		
		
//		for(int i=0;i<candidateItemsets.size();i++){
//			if(flags[i]){
//				
//				find = true;
//				for (int item : candidateItemsets.get(i)) {
//					if (!basket.contains(item)) {
//						find = false;
//						//marco tutti i candidate itemset che contengono item
//						for(int row : index.get(item)){
//							flags[row] = false; 
//						}
//						break;
//					}
//				}
//				if (find) {
//					counters[i]++;
//				}
//				
//			}
//		}
		
		int i=0, item;
		while( i < candidateItemsets.size() && candidateItemsets.get(i).size() == 1){
			item = candidateItemsets.get(i).get(0);
			if (!basket.contains(item)) {
				//marco tutti i candidate itemset che contengono item
				for(int row : index.get(item)){
					flags[row] = false; 
				}
			}
			else{
				counters[i]++;
			}
			i++;
		}
		
		for(; i < candidateItemsets.size(); i++){
			if(flags[i]){
				counters[i]++;
			}
		}		
	}

	public static Vector<Integer> createBasket(String text) {
		StringTokenizer st = null;
		Vector<Integer> items = new Vector<Integer>();

		st = new StringTokenizer(text); // Tokenizzo il basket.

		while (st.hasMoreTokens()) {
			items.add(Integer.parseInt(st.nextToken())); // Aggiungo ogni item
															// al vettore.
		}

		Collections.sort(items);
		return items;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
		Itemset app = new Itemset();
		IntWritable count = new IntWritable();
				
		for(int i=0;i<counters.length;i++){
			if(counters[i]>0){
				app.set(candidateItemsets.get(i));
				count.set(counters[i]);
				context.write(app, count);
			}
		}		
	}
}