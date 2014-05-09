/**
 * 
 */
package SON;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * @author andrea
 *
 */
public class Apriori {

	/**
	 * 
	 */
	/*public Apriori(File file) {
		// TODO Auto-generated constructor stub
		HashMap<Integer, Integer> frequentItem = new HashMap<Integer,Integer>();
		
		HashMap<Vector<Integer>,Integer> kTone = new HashMap<Vector<Integer>,Integer>();
		
		File name = file;
		
		Vector<String> chunk = new Vector<String>();
		
		try {
			BufferedReader input = new BufferedReader(new FileReader(name));
			String text;
			while ((text = input.readLine()) != null)
				chunk.addElement(text);
			
			input.close();

			} catch (IOException ioException) {
				System.out.println("ERRORE LETTURA DA FILE.");
				System.out.println(ioException);
			}
		int i;
		for(i=0; i < chunk.size(); i++)
			System.out.println(chunk.get(i));
			
		System.out.println(chunk.size());
	}
	*/
	
	public Apriori(File file) {
		
		HashMap<Integer, Integer> frequentItem = new HashMap<Integer,Integer>();
		
		//HashMap<Vector<Integer>,Integer> kTone = new HashMap<Vector<Integer>,Integer>();
		
		Vector<HashMap<Vector<Integer>,Integer>> frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
		
		Vector<Integer> basket = new Vector<Integer>();
		
		File name = file;
		
		int basketReaded = 0;
		
		int s = 80;
		
		int k = 0;
		
		try {
			BufferedReader input = new BufferedReader(new FileReader(name));
			String text;
			StringTokenizer st;
			
			while ((text = input.readLine()) != null){
				
				basketReaded++;
				
				st = new StringTokenizer(text);
				basket.clear();
				
				while (st.hasMoreTokens()) {
			         basket.add(Integer.parseInt(st.nextToken()));
		         }
				
				for(int item: basket){
					if(frequentItem.containsKey(item)){
						frequentItem.put(item, frequentItem.get(item)+1);
					}
					else{
						frequentItem.put(item, 1);
					}
					
				}
				
			}
			
			input.close();
			

			} catch (IOException ioException) {
				System.out.println("ERRORE LETTURA DA FILE.");
				System.out.println(ioException);
			}
		
		// Teniamo solo coppie veramente frequenti.
		double frequent = basketReaded*s/100.0;
		
		int singolinonfrequenti=0;				
		for(Integer key: frequentItem.keySet()){
			if(frequentItem.get(key) < frequent){
				frequentItem.put(key, 0);
				singolinonfrequenti++;
			}
		}
		
		//secondo passo A-priori : gestione coppie
		try {
			BufferedReader input = new BufferedReader(new FileReader(name));
			String text;
			StringTokenizer st;

			frequentItemset.add(new HashMap<Vector<Integer>,Integer>());

			while ((text = input.readLine()) != null){
				
				st = new StringTokenizer(text);
				basket.clear();
				
				while (st.hasMoreTokens()) {
			         basket.add(Integer.parseInt(st.nextToken()));
		         }
				
				for(int i=basket.size()-1; i >= 0; i--){
					if(frequentItem.get(basket.get(i))==0){
						basket.remove(i);
					}	
				}
				
				Vector<Integer> tmp;
				for(int i: basket){
					for(int j: basket){
						if(i!=j){
							tmp = new Vector<Integer>();
							tmp.add(i);
							tmp.add(j);
							
							if(!frequentItemset.get(k).containsKey(tmp))
								frequentItemset.get(k).put(tmp, 1);
							else{
								frequentItemset.get(k).put(tmp, frequentItemset.get(k).get(tmp)+1);
							}
						}
						else {
							
						}
					}
				}
				
				
			}
			
			input.close();
			

			} catch (IOException ioException) {
				System.out.println("ERRORE LETTURA DA FILE.");
				System.out.println(ioException);
			}
		
		int coppienonfreqquenti=0;
		for(Vector<Integer> pair :frequentItemset.get(k).keySet()){
			if(frequentItemset.get(k).get(pair) < frequent){
				frequentItemset.get(k).put(pair,0);
				coppienonfreqquenti++;
			}		
		}
		
		System.out.println("Singletone: "+(frequentItem.keySet().size()-singolinonfrequenti)+" DoubleTone: "+(frequentItemset.get(k).size()-coppienonfreqquenti));
		
	}

}
