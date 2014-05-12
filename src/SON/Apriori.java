/**
 * 
 */
package SON;

import java.io.File;
import java.util.HashMap;
import java.util.Vector;

import sun.security.provider.SystemSigner;

/**
 * @author andrea
 *
 */
public class Apriori {
	
	HashMap<Integer, Integer> frequentItem;
	
	public Apriori(File file) {
		
		frequentItem = new HashMap<Integer,Integer>();
		
		//HashMap<Vector<Integer>,Integer> kTone = new HashMap<Vector<Integer>,Integer>();
		
		Vector<HashMap<Vector<Integer>,Integer>> frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
		
		Vector<Integer> basket;
		
		BasketReader br = new BasketReader(file.getAbsolutePath());
		
		HashMap<Vector<Integer>,Integer> Ck, prevCk;
		
		
		int basketReaded = 0;
		
		int s = 30;
		
		int k = 1;
		
		int count=0;
		
		//gestione singletone
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
		System.out.println(frequent);
		
		// Teniamo solo item veramente frequenti.
		int singolinonfrequenti=0;				
		for(Integer key: frequentItem.keySet()){
			if(frequentItem.get(key) < frequent){
				frequentItem.put(key, 0);
				singolinonfrequenti++;
			}
		}
		
		//secondo passo A-priori : gestione coppie	
		k=2;
		frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
		Ck = frequentItemset.get(frequentItemset.size() -1);
		
		br.reset();
		while((basket= br.nextBasket())!= null){
			
			this.preProcessingBasket(basket,k);
			
			//genero tutte le possibili coppie
			Vector<Integer> coppia;
			Generatore g= new Generatore(basket,2);
			while((coppia=g.next()) != null){
				if(!Ck.containsKey(coppia))
					Ck.put(coppia, 1);
				else{
					Ck.put(coppia, Ck.get(coppia)+1);
				}
			}
			
			
		}
		
		//teniamo solo le coppie frequenti	
		int coppienonfreqquenti=0;
		for(Vector<Integer> pair :Ck.keySet()){
			if(Ck.get(pair) < frequent){
				Ck.put(pair,0);
				coppienonfreqquenti++;
			}
			else
				count++;
		}
		
		System.out.println("Singletone: "+(frequentItem.keySet().size()-singolinonfrequenti)+" DoubleTone: "+(Ck.size()-coppienonfreqquenti));
		
		//gestione itemset di dimensione k>2
		Vector<Integer> kTone, prevKtone;
		Generatore genK, prevGenK;
		boolean findNofrequnet;
		while(count >=2 ){
			k++;
			// TODO: eliminare da frequentItemset gli itemset di dimensione k-2 tranne i singleton
			frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
			Ck = frequentItemset.get(frequentItemset.size() - 1 );
			prevCk = frequentItemset.get(frequentItemset.size() - 2 );
			
			br.reset();
			while((basket= br.nextBasket())!= null){
				
				this.preProcessingBasket(basket,k,prevCk);
				
				//genero tutti i possibili itemset di dimensone k
				genK = new Generatore(basket,k);
				
				while((kTone=genK.next()) != null){
					//questo nuovo itemset viene preso in considerazione solo se tutti i sui sottoinsiemi di dimensione k-1 sono frequenti
					prevGenK=new Generatore(kTone,k-1);
					findNofrequnet=false;
					while(!findNofrequnet && (prevKtone=prevGenK.next()) != null ){
						findNofrequnet = !prevCk.containsKey(prevKtone) || prevCk.get(prevKtone) == 0;
					}
							
					
					if(!findNofrequnet){
						
						//kTone ha superato il test
						if(!Ck.containsKey(kTone))
							Ck.put(kTone, 1);
						else{
							Ck.put(kTone, Ck.get(kTone)+1);
						}
						
					}
					
					
				}
				
				
				
		
			}
			
			count=0;
			for(Vector<Integer> itemset :Ck.keySet()){
				if(Ck.get(itemset) < frequent){
					Ck.put(itemset,0);
				}
				else{
					count++;
					for(int item: itemset)
						System.out.print(item+" ");
					System.out.println();
				}
			}
			
			System.out.println(k+"-tone trovati: "+count);
			
		}
		
		
	}

	private void preProcessingBasket(Vector<Integer> basket, int k){
		int sum;
		
		//rimuovo dal basket gli item che non sono frequenti
		for(int i=basket.size()-1; i >= 0; i--){
			if(frequentItem.get(basket.get(i))==0){
				basket.remove(i);
			}	
		}
		
		
}



	
	private void preProcessingBasket(Vector<Integer> basket, int k, HashMap<Vector<Integer>,Integer> prevCk){
		int sum;
		
		//rimuovo dal basket gli item che non sono frequenti
		for(int i=basket.size()-1; i >= 0; i--){
			if(frequentItem.get(basket.get(i))==0){
				basket.remove(i);
			}	
		}
		
		if(k>2){
			//per ogni elemento devo verificare se  presente il almeno k degli k-1 - itemset
			for(int i=basket.size()-1; i >= 0; i--){
				sum=0;
				for(Vector<Integer> itemset :prevCk.keySet()){
					if(itemset.contains(basket.get(i))){
						sum++;
					}
					if(sum>=k) break;
				}
				if(sum<k)
					basket.remove(i);
				
			}
			
		}
		
	}
}


