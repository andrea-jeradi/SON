package FrequentItemset;

import java.util.Vector;
/**
 * Questa classe si occupa di generare le possibili combinazioni di item di lungheza k da un determinato Vettore di basket.
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class Generatore {
	private int index;
	private int n;
	private int k;
	private Vector<Integer> set;
	Integer combinazione[];
	/**
	 * Genera tutte le possibili combinazioni di k elementi contenute nel basket
	 * @param set insieme che rappresenta il basket da cui generare le combinazioni.
	 * @param k lunghezza delle combinazioni di item contenuti in set da generare.
	 */
	public Generatore(Vector<Integer> set, int k) {
		this.set = set;
		this.n = set.size();
		this.k = k;
		
		combinazione = new Integer[k+1];
		
		for(int i=1; i<=k; i++)
			combinazione[i] = i;       
	    combinazione[k] --; //trucco per far si che la prima comb venga generata all'interno del ciclo es. 1 combinazione: 1,2,3  diventerebbe 1,2,2
	    
	    this.index = k;
	}
	/**
	 * Ritorna la combinazione successiva.
	 * @return la combinazione successiva, null se non ce ne sono pi&ugrave
	 */
	public Vector<Integer> next(){
		while (index > 1 || combinazione[1] < (n - k + 1)){
	        if (combinazione[index] < n - (k - index)) { //posso incrementare in questa posizione
	            combinazione[index] ++;
	            if (index != k) {
	                combinazione[index + 1] = combinazione[index];
	                index ++;
	            }
	            else{ //qui ho una nuova combinazione;
	            	Vector<Integer> kItemSet = new Vector<Integer>();
	            	for(int i=1; i<=k; i++)
	            		kItemSet.add(set.get(combinazione[i]-1));
	            	
	            	return kItemSet;
	            }
	        }
	        else //torno indietro
	            index --;
		}
		return null;
	}	
    
}
