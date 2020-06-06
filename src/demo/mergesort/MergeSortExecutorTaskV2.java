package demo.mergesort;

import demo.MergeDemo;

import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class MergeSortExecutorTaskV2 implements MergeDemo.MergeSort {
    private static final int THRESHOLD = 8;
    private ExecutorService executorService;
    private BlockingDeque<Runnable> blockingDeque = new LinkedBlockingDeque<>();

    public MergeSortExecutorTaskV2(int parallelism) {
        executorService = Executors.newFixedThreadPool(parallelism);
    }

    public void sort(int[] array) throws Exception {
        BaseMergeTask baseMergeTask = new BaseMergeTask(1) {
            @Override
            public void run() {
            }
        };
        executorService.submit(new SplitTask(0, array.length, array, blockingDeque, baseMergeTask));

        while (true) {
            Runnable runnable = blockingDeque.takeLast();
            if (baseMergeTask == runnable) {
                return;
            }
            executorService.submit(runnable);
        }
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    public static class SplitTask implements Runnable {
        private int low, high;
        private int[] array;
        private Deque<Runnable> deque;
        private BaseMergeTask parentMerge;

        public SplitTask(int low, int high, int[] array, Deque<Runnable> deque, BaseMergeTask parentMerge) {
            this.low = low;
            this.high = high;
            this.array = array;
            this.deque = deque;
            this.parentMerge = parentMerge;
        }

        @Override
        public void run() {
            if (high - low <= THRESHOLD) {
                Arrays.sort(array, low, high);
                if (parentMerge.decrementCounter() == 0)
                    deque.addFirst(parentMerge);
            } else {
                int middle = low + ((high - low) >> 1);

                MergeTask mergeTask = new MergeTask(low, high, middle, array, deque, parentMerge);
                SplitTask left = new SplitTask(low, middle, array, deque, mergeTask);
                SplitTask right = new SplitTask(middle, high, array, deque, mergeTask);

                deque.addFirst(left);
                deque.addFirst(right);
            }
        }

        @Override
        public String toString() {
            return "SplitTask{" +
                    "low=" + low +
                    ", high=" + high +
                    ", array=" + Arrays.toString(array) +
                    '}';
        }
    }

    public static abstract class BaseMergeTask implements Runnable {
        private final AtomicInteger counter;

        BaseMergeTask(int intialValue) {
            counter = new AtomicInteger(intialValue);
        }

        protected int decrementCounter() {
            return counter.decrementAndGet();
        }

    }

    public static class MergeTask extends BaseMergeTask {
        private int low, high, middle;
        private int[] array;
        private Deque<Runnable> deque;
        private BaseMergeTask parentMergeTask;

        public MergeTask(int low, int high, int middle, int[] array, Deque<Runnable> deque, BaseMergeTask parentMergeTask) {
            super(2);
            this.low = low;
            this.high = high;
            this.middle = middle;
            this.array = array;
            this.deque = deque;
            this.parentMergeTask = parentMergeTask;
        }

        @Override
        public void run() {
            if (array[middle - 1] >= array[middle]) {
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
            if (parentMergeTask.decrementCounter() == 0) {
                deque.addFirst(parentMergeTask);
            }
        }

        @Override
        public String toString() {
            return "MergeTask{" +
                    "low=" + low +
                    ", high=" + high +
                    ", middle=" + middle +
                    '}';
        }
    }
}
