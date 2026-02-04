package poc;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public final class Benchmark {
    public static void run(int workers, int tasks, int keySpace) throws Exception {
        var pool = Executors.newFixedThreadPool(workers);
        try {
            var inputs = generateInputs(tasks, keySpace);

            var single = runCase("single-thread", () -> runSingle(inputs, keySpace));
            var sync = runCase("synchronized", () -> runSynchronized(pool, inputs, keySpace));
            var chm = runCase("concurrent-hashmap", () -> runConcurrentHashMap(pool, inputs, keySpace));
            var striped = runCase("striped-locks", () -> runStriped(pool, inputs, keySpace));

            var ref = single.sum;
            if (ref != sync.sum || ref != chm.sum || ref != striped.sum) {
                throw new IllegalStateException("mismatched sums: " + ref + " " + sync.sum + " " + chm.sum + " " + striped.sum);
            }

            System.out.println();
            System.out.printf("workers=%d tasks=%d keySpace=%d%n", workers, tasks, keySpace);
            print(single);
            print(sync);
            print(chm);
            print(striped);
        } finally {
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static void print(Result r) {
        System.out.printf("%-18s time=%6d ms  ops/s=%10d  sum=%d%n", r.name, r.millis, r.opsPerSec, r.sum);
    }

    private static Result runCase(String name, Callable<Long> body) throws Exception {
        for (int i = 0; i < 2; i++) body.call();
        long start = System.nanoTime();
        long sum = body.call();
        long end = System.nanoTime();
        long ms = TimeUnit.NANOSECONDS.toMillis(end - start);
        long ops = ms == 0 ? -1 : (long) (1_000L * (double) sumOpsHint() / (double) ms);
        return new Result(name, ms, ops, sum);
    }

    private static long sumOpsHint() {
        return 200_000L;
    }

    private static int[] generateInputs(int tasks, int keySpace) {
        var rnd = new SplittableRandom(42);
        var a = new int[tasks];
        for (int i = 0; i < tasks; i++) a[i] = rnd.nextInt(keySpace);
        return a;
    }

    private static long runSingle(int[] inputs, int keySpace) {
        long[] counts = new long[keySpace];
        for (int k : inputs) counts[k]++;
        long sum = 0;
        for (long c : counts) sum += c;
        return sum;
    }

    private static long runSynchronized(ExecutorService pool, int[] inputs, int keySpace) throws Exception {
        Object lock = new Object();
        long[] counts = new long[keySpace];
        var futures = submitBatches(pool, inputs, k -> {
            synchronized (lock) {
                counts[k]++;
            }
        });
        for (var f : futures) f.get();
        long sum = 0;
        for (long c : counts) sum += c;
        return sum;
    }

    private static long runConcurrentHashMap(ExecutorService pool, int[] inputs, int keySpace) throws Exception {
        var map = new ConcurrentHashMap<Integer, LongAdder>(keySpace);
        var futures = submitBatches(pool, inputs, k -> map.computeIfAbsent(k, kk -> new LongAdder()).increment());
        for (var f : futures) f.get();
        long sum = 0;
        for (var e : map.values()) sum += e.sum();
        return sum;
    }

    private static long runStriped(ExecutorService pool, int[] inputs, int keySpace) throws Exception {
        int stripes = 64;
        Object[] locks = new Object[stripes];
        for (int i = 0; i < stripes; i++) locks[i] = new Object();
        long[] counts = new long[keySpace];
        var futures = submitBatches(pool, inputs, k -> {
            Object lock = locks[k & (stripes - 1)];
            synchronized (lock) {
                counts[k]++;
            }
        });
        for (var f : futures) f.get();
        long sum = 0;
        for (long c : counts) sum += c;
        return sum;
    }

    private static List<Future<?>> submitBatches(ExecutorService pool, int[] inputs, IntConsumer consumer) {
        int n = inputs.length;
        int workers = ((ThreadPoolExecutor) pool).getCorePoolSize();
        int batchSize = Math.max(1, n / (workers * 8));
        var futures = new ArrayList<Future<?>>();
        for (int start = 0; start < n; start += batchSize) {
            int s = start;
            int e = Math.min(n, start + batchSize);
            futures.add(pool.submit(() -> {
                for (int i = s; i < e; i++) consumer.accept(inputs[i]);
            }));
        }
        return futures;
    }

    @FunctionalInterface
    private interface IntConsumer {
        void accept(int v);
    }

    private record Result(String name, long millis, long opsPerSec, long sum) {}
}
