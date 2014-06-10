package FrequentItemset;

import java.util.Vector;

public class Utils {
	
	public static void orderItemset( Vector<Vector<Integer>> itemset){
		quickSort(itemset, 0, itemset.size()-1);
	}

	private static void quickSort(Vector<Vector<Integer>> arr, int left, int right) {

		int index = partition(arr, left, right);

		if (left < index - 1){
			quickSort(arr, left, index - 1);
		}
		if (index < right){
			quickSort(arr, index, right);
		}
	}
	private static int partition(Vector<Vector<Integer>> arr, int left, int right) {
		
		int i = left, j = right;
		Vector<Integer> tmp;
		Vector<Integer> pivot = arr.get((left + right) / 2);

		while (i <= j) {
			while (compare(arr.get(i), pivot) < 0){// less
				i++;
			}
			while (compare(arr.get(j), pivot) > 0){// greater
				j--;
			}
			if (i <= j) {
				tmp = arr.get(i);
				arr.set(i, arr.get(j));
				arr.set(j, tmp);

				i++;
				j--;
			}
		}
		return i;
	}
	
	public static int compare(Vector<Integer> is1, Vector<Integer> is2) {
		if (is1.size() != is2.size())
			return is1.size() - is2.size();

		for (int i = 0; i < is1.size(); i++) {
			if (!is1.get(i).equals(is2.get(i))) {
				return is1.get(i) - is2.get(i);
			}
		}

		return 0;
	}
	
	
}
