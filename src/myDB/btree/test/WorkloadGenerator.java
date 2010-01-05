package myDB.btree.test;

import java.util.Random;

import myDB.btree.util.BTreeMap;
import myDB.btree.util.DummyIntPushOperator;
import myDB.btree.util.DummyMap;
import myDB.btree.util.IntPushOperator;
import myDB.btree.util.Map;

/**
 * Workload generator for ACM contest. Note: this workload generator will not
 * generate duplicate entries. This workload generator will be used to measure
 * the performance of your implementation. Note that the time to generate the
 * workload is subtracted from the actual time to perform operations on the
 * index. If you are using C/C++ wou will need to measure times exactly in the
 * same way (i.e., copy this code and translate it to C/C++).
 * 
 * @author Jens Dittrich
 */
public class WorkloadGenerator {

	final static boolean verbose = false;

	final static int maxKey = 10 * 1000 * 1000;

	final static int maxRangeSize = 1000;

	private boolean[] keyBitMap = null;

	private int currentIndexSize = 0;

	private Random rand;

	public WorkloadGenerator(long seed) {
		keyBitMap = new boolean[maxKey + 1];
		rand = new Random(seed);
	}

	public void executeWorkload(final int numberOfOperations, double isQueryProbability,
			double isRangeQueryProbability, double deleteOverInsertProbability, Map myIndex, IntPushOperator results,
			boolean suppressWarnings) {
		int lowKey = -1, highKey = -1, value = -1;
		for (int i = 0; i < numberOfOperations; i++) {

			lowKey = rand.nextInt(maxKey);

			// throw a coin to decide whether we have a query:
			if (rand.nextDouble() <= isQueryProbability) {
				// throw a coin to decide whether we have a range query:
				if (rand.nextDouble() <= isRangeQueryProbability) {
					highKey = Math.min(lowKey + rand.nextInt(maxRangeSize), maxKey);
					if (verbose)
						System.out.print("RQ " + lowKey + " " + highKey);
					// execute range query on index:
					myIndex.rangeQuery(lowKey, highKey, results);
				} else {
					if (verbose)
						System.out.print("PQ " + lowKey);
					// execute range query on index:
					myIndex.pointQuery(lowKey, results);
				}
			} else {
				value = 42000000 + rand.nextInt(1000000);
				// throw a coin to decide whether we have an insert:
				if (rand.nextDouble() <= deleteOverInsertProbability) {
					// delete operation:
					// does this element exist in the index?:
					if (keyBitMap[lowKey]) {
						// delete the entry:
						if (verbose)
							System.out.print("D " + lowKey);
						// execute delete on index:
						myIndex.delete(lowKey);
						keyBitMap[lowKey] = false;
						currentIndexSize--;
					} else {
						// ignore this delete
					}
				} else {
					// insert operation:
					for (int probing = 0; probing < 10; probing++) {
						// linear probing similar to hash table probing:
						int _lowKey = (lowKey + probing) % (maxKey + 1);
						if (!keyBitMap[_lowKey]) {
							if (verbose)
								System.out.print("I " + _lowKey + " -> " + value);
							// execute insert on index:
							myIndex.insert(_lowKey, value);
							keyBitMap[_lowKey] = true;
							currentIndexSize++;
							break;
						}
					}
				}
			}
			if (verbose)
				System.out.println();
		}
		System.out.println("# desired tree size:\t" + currentIndexSize + "\ttree size reported by implementation:\t"
				+ myIndex.size());
		if (!suppressWarnings & currentIndexSize != myIndex.size()) {
			System.err.println("WARNING: TREE SIZE OF IMPLEMENTATION NOT CORRECT!");
		}
	}

	public static void main(String[] args) {

		// define maps to use in different rounds:
		// two different maps: first map (DummyMap) is used to subtract time for
		// workload generation from index execution time; second map
		// (studentIndex) is the actual implementation provided by the student
		// team

		// REPLACE THIS WITH YOUR INDEX IMPLEMENTATION:
		// Map studentIndex = new JavaTreeMap();
		Map studentIndex = new BTreeMap(16, 16);

		Map[] myIndex = new Map[] { new DummyMap(), studentIndex };
		long[] totalTime = new long[2];

		// first round measures time to generate workload
		// second round measures time to generate workload plus index time
		// result is defined as second round minus first round
		for (int round = 0; round < 2; round++) {

			// init generator:
			WorkloadGenerator workloadGenerator = new WorkloadGenerator(42);
			IntPushOperator results = new DummyIntPushOperator();

			// execute first workload to fill the tree (mainly inserts):

			// number of operations performed:
			// you should start with a small number
			// for the measurement we will use at least 10M operations
			int numberOfOperations = 1 * 1000 * 1000;

			// amount of operations that are queries:
			double isQueryProbability = 0.0;
			// amount of all queries that are range queries:
			double isRangeQueryProbability = 0.5;
			// amount of non-queries (modification) that are delte operations:
			double deleteOverInsertProbability = 0.0;
			workloadGenerator.executeWorkload(numberOfOperations, isQueryProbability, isRangeQueryProbability,
					deleteOverInsertProbability, myIndex[round], results, round == 0);

			// start measurement:
			// execute second workload (mix of inserts, deletes, and queries):
			long begin = System.nanoTime();

			numberOfOperations *= 10;
			// amount of operations that are queries:
			isQueryProbability = 0.7;
			// amount of all queries that are range queries:
			isRangeQueryProbability = 0.6;
			// amount of non-queries (modification) that are delte operations:
			deleteOverInsertProbability = 0.5;
			workloadGenerator.executeWorkload(numberOfOperations, isQueryProbability, isRangeQueryProbability,
					deleteOverInsertProbability, myIndex[round], results, round == 0);

			totalTime[round] = System.nanoTime() - begin;
		}
		System.out.println("Time: " + (double) (totalTime[0]) / 1e9 + "sec");
		System.out.println("Time: " + (double) (totalTime[1]) / 1e9 + "sec");
		// final time for your index:
		System.out.println("Total time for " + studentIndex.getClass().getName() + ": " + (totalTime[1] - totalTime[0])
				/ 1e9 + "sec");
	}
}
