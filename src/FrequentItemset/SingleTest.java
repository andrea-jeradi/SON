package FrequentItemset;

import java.io.File;

public class SingleTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		File name = new File("/home/student/Downloads/sample/prova2.txt");
		Apriori a= new Apriori(name,40);
		a.start();
		
		
		
	}
	
	

	
	
	

}

