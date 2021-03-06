package SON;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import FrequentItemset.Utils;
/**
 * Questa classe costruisce un itemset di tipo WritableComparable poichè sono le chiavi utilizzate da Hadoop
 * 
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class Itemset implements WritableComparable<Itemset> {

	private Vector<Integer> items;

	public Itemset() {
		this.items = new Vector<Integer>();
	}

	public Itemset(Vector<Integer> items) {
		this.items = items;
	}

	public void set(Vector<Integer> items) {
		this.items = items;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		IntWritable app = new IntWritable();

		app.set(items.size());
		app.write(out);

		for (int item : items) {
			app.set(item);
			app.write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		items.clear();

		IntWritable app = new IntWritable();
		app.readFields(in); // ho letto la dimensione dell' itemset

		int size = app.get();

		for (int i = 0; i < size; i++) {
			app.readFields(in);
			items.add(app.get());
		}

	}

	@Override
	public int hashCode() {
		return items.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Itemset) {
			Itemset is = (Itemset) o;
			return this.items.equals(is.items);
		}
		return false;
	}

	@Override
	public int compareTo(Itemset is) {
		return Utils.compare(this.items , is.items);
	}

	@Override
	public String toString() {
		String s = "";
		for (int item : items)
			s = s + item + " ";
		return s;
	}

}
