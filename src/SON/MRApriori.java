package SON;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import FrequentItemset.Apriori;
/**
 * Questa classe implementa Apriori per eseguirlo su Hadoop. Sovrascrive il metodo postProcessingItemset
 * della classe FrequentItemset.Apriori
 * 
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class MRApriori extends Apriori {

	Context context;
	/**
	 * Costruttore della classe.
	 * @param file File contenente il dataset da analizzare.
	 * @param s soglia di frequenza con cui gli item devono comparire per risultare frequenti. 
	 * @param context contesto nel quale scrivere l'output.
	 */
	public MRApriori(File file, double s, Context context) {
		super(file, s);
		this.context = context;
	}

	@Override
	protected void postProcessingItemset(HashMap<Vector<Integer>, Integer> Ck) {
		Itemset app = new Itemset();
		IntWritable one = new IntWritable(1);

		for (Vector<Integer> itemset : Ck.keySet()) {
			app.set(itemset);
			try {
				context.write(app, one);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
