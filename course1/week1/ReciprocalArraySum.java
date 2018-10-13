package edu.coursera.parallel

import java.util.concurrent.*;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive,
                               final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            // TODO
            value = 0;
            for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                value += 1 / input[i];
            }
        }

    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        int size = input.length;
        assert size % 2 == 0;

        ReciprocalArraySumTask lo = new ReciprocalArraySumTask(0, size / 2, input);
        ReciprocalArraySumTask hi = new ReciprocalArraySumTask(size / 2, input.length, input);

        lo.fork();
        hi.compute();
        lo.join();

        return lo.getValue() + hi.getValue();
    }

    private static class MultiSumArray extends RecursiveAction {

        static int THRESHOLD = 400000;
        int numTasks;
        int lo;
        int hi;
        double[] arr;
        double ans = 0;

        MultiSumArray(double[] a, int l, int h, int n) {
            arr = a;
            hi = h;
            lo = l;
            numTasks = n;
        }

        @Override
        protected void compute() {
            if (hi - lo <= THRESHOLD) {
                for (int i = lo; i < hi; i++) {
                    ans += 1 / arr[i];
                }
            } else {
                MultiSumArray[] arrays = new MultiSumArray[numTasks];
                int curr = 0;
                int span = (hi - lo) / numTasks;
                for (int n = 0; n < numTasks; n++) {
                    arrays[n] = new MultiSumArray(arr, lo + curr, lo + curr + span, numTasks);
                    curr += span;
                }

                for (int i = 0; i < numTasks - 1; i++) {
                    arrays[i].fork();
                }

                arrays[arrays.length - 1].compute();

                for (int i = 0; i < numTasks - 1; i++) {
                    arrays[i].join();
                }

                for (int i = 0; i < numTasks; i++) {
                    ans += arrays[i].ans;
                }
            }
        }
    }

        /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        int size = input.length;
        int taskNum = numTasks;

        if (taskNum > size){
            taskNum = size;
        }

        ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[taskNum];

        for(int i = 0; i < taskNum - 1; i++){
            tasks[i] = new ReciprocalArraySumTask(getChunkStartInclusive(i, taskNum, size),
                    getChunkEndExclusive(i, taskNum, size), input);
            tasks[i].fork();
        }
        tasks[taskNum - 1] = new ReciprocalArraySumTask(getChunkStartInclusive(taskNum - 1, taskNum, size),
                getChunkEndExclusive(taskNum - 1, taskNum, size), input);
        tasks[taskNum - 1].compute();

        for(int j = 0; j < taskNum-1; j++){
            tasks[j].join();
        }

        double sum = 0;
        for(int j = 0; j < taskNum; j++){
            sum += tasks[j].getValue();
        }
        return sum;
    }
}
