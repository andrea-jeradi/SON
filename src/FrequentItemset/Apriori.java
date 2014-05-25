/**
 * 
 */
package FrequentItemset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Vector;

/**
 * @author andrea
 *
 */
public class Apriori {
	
	//private HashMap<Integer, Integer> frequentItem;
	private Vector<HashMap<Vector<Integer>,Integer>> frequentItemset;
	private HashMap<Vector<Integer>,Integer> Ck, prevCk;
	BasketReader br;
	double frequent;
	int s;
	
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	BufferedWriter  bw;
	
	public Apriori(File file,int s) {
		
		this.s = s;
		
		//frequentItem = new HashMap<Integer,Integer>();
		
		//HashMap<Vector<Integer>,Integer> kTone = new HashMap<Vector<Integer>,Integer>();
		
		frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
		
		
		
		br = new BasketReader(file.getAbsolutePath());
		
	}
	
	public Apriori(Vector<Vector<Integer>> baskets,int s) {
		
		this.s = s;
	
		
		frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
		
		
		
		br = new BasketReader(baskets);
		
	}
	
public Apriori(Vector<Vector<Integer>> baskets,int s, BufferedWriter out) {
		
		this.s = s;
	
		
		frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
		
		
		
		br = new BasketReader(baskets);
		
		this.bw= out; 
		
	}
	
	
	
	public void start() throws IOException{	
		
		int basketReaded = 0;
		int k = 1;
		int count=0;
		
		Vector<Integer> basket;
		
		//gestione singletone
		k=1;
		frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
		Ck = frequentItemset.get(frequentItemset.size() -1);
		while((basket= br.nextBasket())!= null){
			basketReaded++;
			
			//genero tutte le possibili coppie
			Vector<Integer> item;
			Generatore g= new Generatore(basket,k);
			while((item=g.next()) != null){
				if(!Ck.containsKey(item))
					Ck.put(item, 1);
				else{
					Ck.put(item, Ck.get(item)+1);
				}
			}
			
			
		}
		
		frequent = basketReaded*s/100.0;
		System.out.println("frequenza: "+frequent+"\n");
		System.out.println("basketReaded: "+basketReaded+"\n");
		
		// Teniamo solo item veramente frequenti.
		int singolinonfrequenti=0;				
		for(Vector<Integer> singleton :Ck.keySet()){
			if(Ck.get(singleton) < frequent){
				Ck.put(singleton,0);
				singolinonfrequenti++;
			}
			else
				count++;
		}
		
		System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
				": 1-tone trovati: "+(Ck.size()-singolinonfrequenti+"\n"));
		
		//secondo passo A-priori : gestione coppie	
		k=2;
		frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
		Ck = frequentItemset.get(frequentItemset.size() -1);
		
		br.reset();
		while((basket= br.nextBasket())!= null){
			
			this.preProcessingBasket(basket,k,null);
			
			//genero tutte le possibili coppie
			Vector<Integer> coppia;
			Generatore g= new Generatore(basket,k);
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
		
		System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
				": 2-tone trovati: "+(Ck.size()-coppienonfreqquenti+"\n"));
		
		//gestione itemset di dimensione k>2
		Vector<Integer> kTone, prevKtone;
		Generatore genK, prevGenK;
		boolean findNofrequnet;
		while(count >=2 ){
			k++;
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
						if(bw != null) bw.write(item+" ");
					if(bw != null) bw.write("\n");
				}
			}
			
			System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
								": "+k+"-tone trovati: "+count+"\n");
			
		}
		
		
	}

	/*private void preProcessingBasket(Vector<Integer> basket, int k){
		
		//rimuovo dal basket gli item che non sono frequenti
		for(int i=basket.size()-1; i >= 0; i--){
			if(frequentItem.get(basket.get(i))==0){
				basket.remove(i);
			}	
		}
		
		
}*/



	
	private void preProcessingBasket(Vector<Integer> basket, int k, HashMap<Vector<Integer>,Integer> prevCk){
		int sum;
		Vector<Integer> temp= new Vector<Integer>();temp.add(-1);
		
		//rimuovo dal basket gli item che non sono frequenti
		for(int i=basket.size()-1; i >= 0; i--){			
			temp.set(0, basket.get(i));
			if(frequentItemset.get(0).get(temp)==0){
				basket.remove(i);
			}	
		}
		
		if(k>2){
			//per ogni elemento devo verificare se � presente il almeno k dei frequent itemset di dimensione k-1
			for(int i=basket.size()-1; i >= 0; i--){
				sum=0;
				for(Vector<Integer> itemset :prevCk.keySet()){
					if(itemset.contains(basket.get(i))){
						sum++;
					}
					if(sum>=(k-1)) break;
				}
				if(sum<(k-1))
					basket.remove(i);
				
			}
			
		}
		
	}
	
	public Vector<Vector<Integer>> getCandidateItemset(){
		Vector<Vector<Integer>> res = new Vector<Vector<Integer>>();
		
		for(HashMap<Vector<Integer>,Integer> Ck : frequentItemset){
			for(Vector<Integer> itemset :Ck.keySet()){
				if(Ck.get(itemset) != 0){
					res.add(itemset);
				}
			}
		}
		
		return res;
	}
}


