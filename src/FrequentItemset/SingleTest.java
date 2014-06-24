package FrequentItemset;

import java.io.File;
import java.io.IOException;
import java.util.Vector;
/**
 * Classe che permette l'esecuzione dell'algoritmo Apriori su macchina singola.
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class SingleTest {

	public static void main(String[] args) {
		
		File name = new File(args[0]);
		Apriori a= new Apriori(name,Integer.parseInt(args[1]));
		try {
			a.start();
			for(Vector<Integer> itemset : a.getCandidateItemset()){
				System.out.println(itemset);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}

}

