package SON;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Costruisce il Reducer utilizzato nel primo step dell'algoritmo SON.
 * 
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class MRStep1Reducer extends
		Reducer<Itemset, IntWritable, Itemset, IntWritable> {

	private IntWritable one = new IntWritable(1);

	@Override
	protected void reduce(Itemset key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		context.write(key, one);

	}
}