package demo.mergesort;

import demo.MergeDemo;

import java.util.Arrays;

public class MergeSortSingleThreadTask implements Runnable, MergeDemo.MergeSort {
    private static final int THRESHOLD = 8;
    private int low;
    private int high;
    private int[] array;

    public MergeSortSingleThreadTask(int parallelism) {
    }

    public MergeSortSingleThreadTask(int[] array,
                                     int low,
                                     int high) {
        this.array = array;
        this.low = low;
        this.high = high;
    }

    public void sort(int[] array) throws Exception {
        this.array = array;
        getTask(0, array.length).run();

    }

    @Override
    public void run() {
        if (high - low <= THRESHOLD) {
            Arrays.sort(array, low, high);
        } else {
            int middle = low + ((high - low) >> 1);
            // Trigger Right in next thread
            //Run left in current thread
            getTask(low, middle).run();
            getTask(middle, high).run();

            // Then merge the results
            merge(middle);
        }
    }

    private MergeSortSingleThreadTask getTask(int low, int high) {
        return new MergeSortSingleThreadTask(

                array,
                low,
                high);
    }

    /**
     * Merges the two sorted arrays this.low, middle - 1 and middle, this.high - 1
     *
     * @param middle the index in the array where the second sorted list begins
     */
    private void merge(int middle) {
        if (array[middle - 1] < array[middle]) {
            return; // the arrays are already correctly sorted, so we can skip the merge
        }
        int[] copy = new int[high - low];
        System.arraycopy(array, low, copy, 0, copy.length);
        int copyLow = 0;
        int copyHigh = high - low;
        int copyMiddle = middle - low;

        for (int i = low, p = copyLow, q = copyMiddle; i < high; i++) {
            if (q >= copyHigh || (p < copyMiddle && copy[p] < copy[q])) {
                array[i] = copy[p++];
            } else {
                array[i] = copy[q++];
            }
        }
    }

}
