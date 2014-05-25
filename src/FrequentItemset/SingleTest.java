package FrequentItemset;

import java.io.File;
import java.io.IOException;

public class SingleTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		File name = new File("/home/student/Downloads/sample/blocco.txt");
		Apriori a= new Apriori(name,20);
		try {
			a.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	

	
	
	

}

