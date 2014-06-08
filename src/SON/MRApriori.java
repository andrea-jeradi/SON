package SON;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import FrequentItemset.Apriori;

public class MRApriori extends Apriori {

	Context context;

	public MRApriori(File file, double s, Context context) {
		super(file, s);
		this.context = context;
	}

	@Override
	protected void postProcessingItemset(HashMap<Vector<Integer>, Integer> Ck) {
		Itemset app = new Itemset();
		IntWritable one = new IntWritable(1);

		for (Vector<Integer> itemset : Ck.keySet()) {
			app.set(itemset);
			try {
				context.write(app, one);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
