package FrequentItemset;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

public class SingleTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		File name = new File(args[0]);
		Apriori a= new Apriori(name,Integer.parseInt(args[1]));
		try {
			a.start();
			for(Vector<Integer> itemset : a.getCandidateItemset()){
				System.out.println(itemset);
			}
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	

	
	
	

}

