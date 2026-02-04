package poc;

public class Main {
    public static void main(String[] args) throws Exception {
        int workers = args.length > 0 ? Integer.parseInt(args[0]) : Math.max(2, Runtime.getRuntime().availableProcessors());
        int tasks = 200_000;
        int keySpace = 10_000;
        Benchmark.run(workers, tasks, keySpace);
    }
}
