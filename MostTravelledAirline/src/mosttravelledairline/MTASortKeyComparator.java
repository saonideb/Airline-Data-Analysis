/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mosttravelledairline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author dipti
 */
public class MTASortKeyComparator extends WritableComparator{
    protected MTASortKeyComparator() {
		super(IntWritable.class, true);
	}
    @Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntWritable key1 = (IntWritable) a;
		IntWritable key2 = (IntWritable) b;

		// Implemet sorting in descending order
		int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
		return result;
	}
}
