package demo.mergesort;

import demo.MergeDemo;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MergeSortExecutorTask implements Runnable, MergeDemo.MergeSort {
    private static final int THRESHOLD = 8;
    private final ExecutorService executorService;
    private int low;
    private int high;
    private int[] array;

    public MergeSortExecutorTask(int parallelism) {
        this.executorService = Executors.newFixedThreadPool(parallelism);
    }

    public MergeSortExecutorTask(ExecutorService executorService,
                                 int[] array,
                                 int low,
                                 int high) {
        this.executorService = executorService;
        this.array = array;
        this.low = low;
        this.high = high;
    }

    public void sort(int[] array) throws Exception {
        this.array = array;
        submitTask(0, array.length).get();
    }

    @Override
    public void run() {
        if (high - low <= THRESHOLD) {
            Arrays.sort(array, low, high);
        } else {
            int middle = low + ((high - low) >> 1);
            // Trigger Right in next thread
            Future<?> futureRight = submitTask(middle, high);
            //Run left in current thread
            getTask(low, middle).run();
            //getTask(middle, high).run();

            try {
                futureRight.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Then merge the results
            merge(middle);
        }
    }

    private Future<?> submitTask(int low, int middle) {
        return executorService.submit(getTask(low, middle));
    }

    private MergeSortExecutorTask getTask(int low, int high) {
        return new MergeSortExecutorTask(
                executorService,
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
