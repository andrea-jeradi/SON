package SON;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Itemset implements WritableComparable<Itemset> {

	
	private Vector<Integer> items;

 
	public Itemset() {
		items = new Vector<Integer>();
	}
	
	public Itemset(Vector<Integer> items) {
		this.items = items;
	}
	
	public void set(Vector<Integer> items) {
		this.items = items;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		System.out.println("scrivo itemset di dimensione "+items.size());
		/*out.write(items.size());
		for(int item : items)
			out.write(item);*/
		IntWritable app = new IntWritable();
		
		app.set(items.size());
		app.write(out);
		
		for(int item : items){
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
		
		System.out.println("itemset ha dimensione "+size);
		for(int i=0; i<size;i++){
			app.readFields(in);
			items.add(app.get());
		}
			
	}

	@Override
	public int hashCode() {
//		Integer ar[] = new Integer[]{7,31,13,17,163,37,43};
//		//return items.hashCode();
//		int hashc = 0;
//		for (int i = 0; i < items.size(); i++) {
//			hashc += ar[i] * items.get(i);
//		}
		
		int hashc = 0;//items.hashCode()*163+items.size();
		return hashc;
	}

	@Override
	public boolean equals(Object o) {
		return true;
//		//implement equals
//		System.out.println("SONO EQUALS");
//		if(o instanceof Itemset){
//			Itemset is =(Itemset) o;
//			//return this.items.equals(is.items);
//			if(this.items.equals(is.items)){
//				System.out.println(this+" == "+is);
//				
//					
//			}
//			else
//				System.out.println(this+" != "+is);
//			return this.items.equals(is.items);
//		}
//		return false;
	}
	
	@Override
	public int compareTo(Itemset is) {
		
		//implement the comparison between this and tp
		if(this.items.size() != is.items.size()){
			System.out.println("1SONO compare "+(this.items.size() - is.items.size()));
			return this.items.size() - is.items.size();
		}
		
		for(int i=0; i<this.items.size(); i++){
			if(!this.items.get(i).equals(is.items.get(i))){
				System.out.println("2SONO compare "+(this.items.get(i) - is.items.get(i)));
				return this.items.get(i) - is.items.get(i);
			}
		}
	
		System.out.println("3SONO compare uguali");
		return 0;
		//return this.toString().compareTo(is.toString());
	}
	
	@Override
	public String toString() {
		String s = "";
		for(int item : items)
			s = s + item + " ";
		return s;
	}

}
