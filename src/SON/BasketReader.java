/**
 * 
 */
package SON;

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
	/**
	 * 
	 */
	public BasketReader(String FilePath) {
		// TODO Auto-generated constructor stub
		this.fp = FilePath;
		this.name = new File(fp);
		try {
			input = new BufferedReader(new FileReader(fp));
		} catch (FileNotFoundException e) {
			System.out.println("Errore nella lettura del file: "+fp);
			e.printStackTrace();
		}
		
	}
	
	public Vector<Integer> nextBasket() {
		String text;
		StringTokenizer st = null;
		Vector<Integer> items = new Vector<Integer>();
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
		
		Collections.sort(items);
		return items;

	}
	
	public void reset(){
		try {
			input.close();
		} catch (IOException e) {
			System.out.print("Errore nella chiusura del file.");
			e.printStackTrace();
		}
		this.name = new File(fp);
		
		try {
			input = new BufferedReader(new FileReader(fp));
		} catch (FileNotFoundException e) {
			System.out.println("Errore nella lettura del file: "+fp);
			e.printStackTrace();
		}
		
	}

}
