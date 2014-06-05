package SON;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MRStep2Reducer extends
		Reducer<Itemset, IntWritable, Itemset, IntWritable> {

	double frequent;
	IntWritable count = new IntWritable();

	@Override
	protected void setup(Context context) {
		long basketReaded = context.getConfiguration().getInt("basketReaded",
				Integer.MAX_VALUE);
		double s = context.getConfiguration().getDouble("s", 100);

		frequent = basketReaded * s / 100.0;
	}

	@Override
	protected void reduce(Itemset key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable i : values)
			sum += i.get();

		if (sum >= frequent) {
			count.set(sum);
			context.write(key, count);
		}

	}
}