package SON;

import org.apache.hadoop.io.WritableComparator;
/**
 * Questa classe costruisce un comparator per gli oggetti Itemset.
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class ItemsetComparator extends WritableComparator {

	private int a, b, n;
	/**
	 * Richiama il costruttore della super classe passandogli un oggetto Itemset.
	 */
	protected ItemsetComparator() {
		super(Itemset.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		// primo confronto sulla lunghezza dell'itemset
		a = readInt(b1, s1);
		b = readInt(b2, s2);

		if (a != b) {
			return a - b;
		}
		// secondo confronto elemento per elemento
		n = a;
		for (int i = 1; i <= n; i++) {
			a = readInt(b1, s1 + i * 4);
			b = readInt(b2, s2 + i * 4);
			if (a != b) {
				return a - b;
			}

		}

		return 0;
	}

}
