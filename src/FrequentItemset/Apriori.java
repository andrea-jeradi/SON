/**
 * 
 */
package FrequentItemset;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
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
	
	
	public Apriori(File file,int s) {
		
		this.s = s;
				
		frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
		
		br = new BasketReader(file.getAbsolutePath());
		
	}
	
	public Apriori(Vector<Vector<Integer>> baskets,int s) {
		
		this.s = s;
		
		frequentItemset = new Vector<HashMap<Vector<Integer>,Integer>>();
			
		br = new BasketReader(baskets);
		
	}
	
	public void start() throws IOException{	
		int basketReaded = 0;
		int k = 1;
		boolean check;
		Vector<Integer> basket;
		
		//gestione singletone
		k=1;
		frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
		Ck = frequentItemset.get(frequentItemset.size() -1);
		while((basket= br.nextBasket())!= null){
			basketReaded++;
			
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
		
		Ck = cleanCk();
		frequentItemset.set(frequentItemset.size() -1, Ck);
		
		postProcessingItemset(Ck);
		
		
		System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
				": 1-tone trovati: "+(Ck.size()+"\n"));
		
//		//secondo passo A-priori : gestione coppie	
//		k=2;
//		frequentItemset.add(new HashMap<Vector<Integer>,Integer>());
//		Ck = frequentItemset.get(frequentItemset.size() -1);
//		
//		br.reset();
//		while((basket= br.nextBasket())!= null){
//			
//			this.preProcessingBasket(basket,k,null);
//			
//			//genero tutte le possibili coppie
//			Vector<Integer> coppia;
//			Generatore g= new Generatore(basket,k);
//			while((coppia=g.next()) != null){
//				if(!Ck.containsKey(coppia))
//					Ck.put(coppia, 1);
//				else{
//					Ck.put(coppia, Ck.get(coppia)+1);
//				}
//			}			
//		}
//		
//		//teniamo solo le coppie frequenti	
//		Ck = cleanCk();
//		frequentItemset.set(frequentItemset.size() -1, Ck);
//		
//		System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
//				": 2-tone trovati: "+(Ck.size()+"\n"));
//		System.out.flush();
		
		//gestione itemset di dimensione k>=2
		Vector<Integer> kTone, prevKtone;
		Generatore genK, prevGenK;
		boolean findNofrequnet;
		int c,per,x=basketReaded/10,appr1,appr2; //per vis la perc a video
		while(Ck.size() >= 2 ){
			k++;
			
			c=0;
			per=0;
			appr1=0;
			appr2=0;
			
			prevCk = Ck;
			Ck = generateCandidateItemset(k,prevCk);
			
			System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
					": candidate itemset of size "+k+" = "+Ck.size()+"\n");
			System.out.flush();
			
			br.reset();
			while((basket= br.nextBasket())!= null){
				
				if(c++ % x ==0)
					System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
							": " + (per=per+10) + " %");
				
				
				this.preProcessingBasket(basket,k,prevCk);
				
				
				
				//scelgo quale approccio usare in base alla grandezza del busket 
				if(Ck.size() <= combinazioni(basket.size(),k)){
					appr1++;
					//Apprioccio 1
					//per ogni candidate itemset verifico se è presente nel basket e incremento il contatore
					for(Vector<Integer> itemset :Ck.keySet()){
						check=true;
						for(int item : itemset){
							if(!basket.contains(item)){
								check=false;
								break;
							}
						}
						if(check){
							Ck.put(itemset, Ck.get(itemset)+1);
						}
					}
				
				}
				else{
//					System.out.println("basket:"+basket.size()+" comb:"+combinazioni(basket.size(),k));
//					System.out.println("uso approccio 2");
					appr2++;
					//Apprioccio 2
					//genero tutti i possibili itemset di dimensone k
					genK = new Generatore(basket,k);
					
					while((kTone=genK.next()) != null){
						//questo nuovo itemset viene preso in considerazione solo se tutti i sui sottoinsiemi di dimensione k-1 sono frequenti
						prevGenK=new Generatore(kTone,k-1);
						findNofrequnet=false;
						while(!findNofrequnet && (prevKtone=prevGenK.next()) != null ){
							findNofrequnet = !prevCk.containsKey(prevKtone);// || prevCk.get(prevKtone) == 0;
						}
								
						
						if(!findNofrequnet){
							
//							//kTone ha superato il test
//							if(!Ck.containsKey(kTone))
//								Ck.put(kTone, 1);
//							else{
								Ck.put(kTone, Ck.get(kTone)+1);
//							}
							
						}
						
						
					}
				}						
			}
		
			Ck = cleanCk();
			frequentItemset.add(Ck);
			
			postProcessingItemset(Ck);
			
			System.out.println("Approccio 1: "+appr1+ " Approccio 2: "+appr2);
			
			System.out.println(dateFormat.format(Calendar.getInstance().getTime())+
								": "+k+"-tone trovati: "+Ck.size()+"\n");
			System.out.flush();
			
			
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

	private int combinazioni(int n, int k){
		int tot = 1;
		
		for(int i=0; i<k; i++){
			tot *= n;
			n--;
		}
		for(;k>1;k--){
			tot /= k;
		}
			
		return tot;
	}

	
	private void preProcessingBasket(Vector<Integer> basket, int k, HashMap<Vector<Integer>,Integer> prevCk){
		int sum;
		//boolean check;
		Vector<Integer> temp= new Vector<Integer>();temp.add(-1);
		
		//rimuovo dal basket gli item che non sono frequenti
		for(int i=basket.size()-1; i >= 0; i--){			
			temp.set(0, basket.get(i));
			//if(frequentItemset.get(0).get(temp)==0){
			if(!frequentItemset.get(0).containsKey(temp)){
				basket.remove(i);
			}	
		}
		
		if(k>2){
			//per ogni elemento devo verificare se è presente il almeno k-1 dei frequent itemset di dimensione k-1
			for(int i=basket.size()-1; i >= 0; i--){
				sum=0;
				for(Vector<Integer> itemset :prevCk.keySet()){
					//if(prevCk.get(itemset) != 0){
						if(itemset.contains(basket.get(i))){
							//itemset contine l'elemento ma il resto del itemset è formato da elemnti presenti nel basket?
							/*check=true;
							for(int item : itemset){
								if(!basket.contains(item)){
									check=false;
									break;
								}		
							}
							if(check){*/
								sum++;
								if(sum>=(k-1)) break;
							//}
						}
						
					//}
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
	
	private HashMap<Vector<Integer>,Integer> cleanCk(){
		HashMap<Vector<Integer>,Integer> newCk = new HashMap<Vector<Integer>,Integer>();
		for(Vector<Integer> itemset :Ck.keySet()){
			if(Ck.get(itemset) >= frequent){
				newCk.put(itemset,Ck.get(itemset));
			}
		}
		return newCk;
		
	}
	
	
	private HashMap<Vector<Integer>,Integer> generateCandidateItemset(int k, HashMap<Vector<Integer>,Integer> prevCk){
		HashMap<Vector<Integer>,Integer> newCk = new HashMap<Vector<Integer>,Integer>();
		
		Vector<Vector<Integer>> frItemset = new Vector<Vector<Integer>>();
		for(Vector<Integer> itemset :prevCk.keySet()){
			frItemset.add(itemset);
		}
		
		boolean check;
		int prevk= k-1;
		
		Vector<Integer> is1,is2;
//		Vector<Integer> is3 = new Vector<Integer>();
//		for(int i=0; i<prevk; is3.add(0),i++);
		
		Vector<Integer> kTone;
		
		for(int i=0; i<frItemset.size();i++){
			for(int j=i+1; j<frItemset.size();j++){
				is1=frItemset.get(i);
				is2=frItemset.get(j);
				
				//System.out.println(is1 + " " + is2);
				//devono avere tutti gli elemnti ugali tranne l'ultimo
				check=true;
				for(int el=0; el<prevk-1; el++){
					if(!is1.get(el).equals(is2.get(el))){
						check=false;
						break;
					}
				}
				if(check){
					//devo verificare che ci sia l'itemset formato da ...
//					for(int el=1; el<prevk; el++){
//						is3.set(el-1, is1.get(el));
//					}
//					is3.set(prevk-1, is2.get(prevk-1));
//					
//					if(true ||frItemset.contains(is3)){
						kTone = new Vector<Integer>();
						for(int el=0; el<prevk; el++){
							kTone.add(is1.get(el));
						}
						kTone.add(is2.get(prevk-1));
						
						//System.out.println("candidato:"+kTone);
						Collections.sort(kTone);
						
						newCk.put(kTone,0);
						
//						//Stampo
//						for(int el : kTone)
//							System.out.print(el+" ");
//						System.out.println();
//					}
				}
				
			}

		}
		return newCk;
	}


	protected void postProcessingItemset(HashMap<Vector<Integer>,Integer> Ck){
		
	}
}