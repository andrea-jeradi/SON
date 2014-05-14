package SON;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ItemsetComparator extends WritableComparator {

	protected ItemsetComparator(Itemset keyClass) {
		super(Itemset.class);
		// TODO Auto-generated constructor stu
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		if(a instanceof Itemset && b instanceof Itemset){
			return a.compareTo(b);
		}
		   return super.compare(a, b);

	}

}
