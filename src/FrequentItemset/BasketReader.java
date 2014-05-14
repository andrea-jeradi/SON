/**
 * 
 */
package FrequentItemset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * @author andrea
 *
 */
public class BasketReader {
	
	private String fp;
	
	private File name;
	
	private BufferedReader input;
	
	private Vector<Vector<Integer>> baskets = null;
	
	private int current;
	/**
	 * 
	 */
	public BasketReader(String FilePath) {
		this.fp = FilePath;
		this.name = new File(fp);
		try {
			input = new BufferedReader(new FileReader(name));
		} catch (FileNotFoundException e) {
			System.out.println("Errore nella lettura del file: "+fp);
			e.printStackTrace();
		}
		
	}
	
	public BasketReader(Vector<Vector<Integer>> b){
		this.baskets = b;
		this.current = 0;
	}
	
	public Vector<Integer> nextBasket() {
		String text;
		StringTokenizer st = null;
		Vector<Integer> items = new Vector<Integer>();
		
		if(baskets!=null){ //Non sto leggengo da file quindi sono nel secondo costruttore.
			if(baskets.size()<=current)
				return null; // Finito la lettura dei baskets.
			items = baskets.get(current); //prendo la riga corrente.
			current++; 
		}
		else{
			// Se ci sono ancora basket nel file.
			try {
				if ((text = input.readLine()) != null) // Leggo un basket.
					st = new StringTokenizer(text); // Tokenizzo il basket.
				else
					return null;
			} catch (IOException e) {
				System.out.println("Errore nella lettura del file: "+fp);
				e.printStackTrace();
			}
			
			while(st.hasMoreTokens()) {
				items.add(Integer.parseInt(st.nextToken())); // Aggiungo ogni item al vettore.
			}
		}// Chiudo else selezione costruttore.
		
		Collections.sort(items);
		return items;

	}
	
	public void reset(){
		if(baskets != null) { // Sono nel secondo costruttore.
			current = 0;
			return;
		}
		else {
			try {
				input.close();
			} catch (IOException e) {
				System.out.print("Errore nella chiusura del file.");
				e.printStackTrace();
			}
			this.name = new File(fp);
			
			try {
				input = new BufferedReader(new FileReader(name));
			} catch (FileNotFoundException e) {
				System.out.println("Errore nella lettura del file: "+fp);
				e.printStackTrace();
			}	
		}
	}

}
