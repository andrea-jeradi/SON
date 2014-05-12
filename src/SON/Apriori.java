/**
 * 
 */
package SON;

import java.io.File;
import java.util.HashMap;
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
		
		Vector<Integer> basket;
		
		BasketReader br = new BasketReader(file.getAbsolutePath());
		
		//File name = file;
		
		int basketReaded = 0;
		
		int s = 80;
		
		int k = 0;
		
		
		
		while((basket= br.nextBasket())!= null){
			basketReaded++;
			for(int item: basket){
				if(frequentItem.containsKey(item)){
					frequentItem.put(item, frequentItem.get(item)+1);
				}
				else{
					frequentItem.put(item, 1);
				}
				
			}
		}
		
		double frequent = basketReaded*s/100.0;
		
		// Teniamo solo item veramente frequenti.
		int singolinonfrequenti=0;				
		for(Integer key: frequentItem.keySet()){
			if(frequentItem.get(key) < frequent){
				frequentItem.put(key, 0);
				singolinonfrequenti++;
			}
		}
		
		//secondo passo A-priori : gestione coppie	
		frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
		br.reset();
		while((basket= br.nextBasket())!= null){
			
			//rimuovo dal basket gli item che non sono frequenti
			for(int i=basket.size()-1; i >= 0; i--){
				if(frequentItem.get(basket.get(i))==0){
					basket.remove(i);
				}	
			}
			
			//genero tutte le possibili coppie
			Vector<Integer> coppia;
			Generatore g= new Generatore(basket,2);
			while((coppia=g.next()) != null){
				if(!frequentItemset.get(k).containsKey(coppia))
					frequentItemset.get(k).put(coppia, 1);
				else{
					frequentItemset.get(k).put(coppia, frequentItemset.get(k).get(coppia)+1);
				}
			}
			
			
		}
		
		//teniamo solo le coppie frequenti	
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
