package SON;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Questa classe costruisce l'oggetto mapper che verr&agrave utilizzato nel primo step di SON.
 * 
 * @author Andrea Jeradi, Francesco Donato
 *
 */
public class MRStep1Mapper extends Mapper<LongWritable, Text, Itemset, IntWritable> {

	Vector<File> chunks;
	int nFile;
	int sizeOfChunk;
	int currentSize;

	BufferedWriter bw;

	@Override
	protected void setup(Context context) throws IOException {
		nFile = 0;
		currentSize = 0;
		chunks = new Vector<File>();
		sizeOfChunk = context.getConfiguration().getInt("sizeOfChunk", -1);
		System.out.println("sizeOfChunk=" + sizeOfChunk);

		chunks.add(new File("Temp" + this + "_" + nFile + ".txt"));
		bw = new BufferedWriter(new FileWriter(chunks.get(nFile)));
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		if (sizeOfChunk != -1 && currentSize > sizeOfChunk) {

			bw.close();

			nFile++;
			currentSize = 0;
			chunks.add(new File("Temp" + this + "_" + nFile + ".txt"));
			bw = new BufferedWriter(new FileWriter(chunks.get(nFile)));
		}
		bw.write(line + "\n");
		currentSize++;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		bw.close();

		double s = context.getConfiguration().getDouble("s", 100);

		for (File f : chunks) {
			System.out.println("nuovo file: " + f.toString());
			GregorianCalendar inizio = new GregorianCalendar();

			MRApriori a = new MRApriori(f, s, context);
			a.start();

			GregorianCalendar fine = new GregorianCalendar();
			System.out.println("tempo exec: "
					+ (fine.getTimeInMillis() - inizio.getTimeInMillis())
					/ 1000 + " sec");

			f.delete();
		}

	}
}
