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

public class MRStep2Mapper extends
		Mapper<LongWritable, Text, Itemset, IntWritable> {

	HashMap<Vector<Integer>, Integer> candidateItemset = new HashMap<Vector<Integer>, Integer>();

	@Override
	protected void setup(Context context) {

		Path path;
		FileSystem fs;
		FSDataInputStream file;
		BufferedReader input;
		FileStatus[] status;
		String text;

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
						candidateItemset.put(createBasket(text), 0);
					}
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
		boolean find;

		for (Vector<Integer> itemset : candidateItemset.keySet()) {
			find = true;
			for (int item : itemset) {
				if (!basket.contains(item)) {
					find = false;
					break;
				}
			}
			if (find) {
				candidateItemset.put(itemset, candidateItemset.get(itemset) + 1);
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
		int temp;

		for (Vector<Integer> itemset : candidateItemset.keySet()) {
			temp = candidateItemset.get(itemset);
			if (temp > 0) {
				app.set(itemset);
				count.set(temp);
				context.write(app, count);
			}
		}

	}
}