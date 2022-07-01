package io.lettuce.core;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.*;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.KeyValueListOutput;
import io.lettuce.core.protocol.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.lettuce.core.protocol.CommandType.MGET;

public class CacheTest {
    private ExecutorService processThreadPool;
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisClusterClient client;
    private RedisAdvancedClusterAsyncCommandsImpl asyncCommands;
    private int typeLevel;
    private int keySize;
    private int valueSize;
    private int threadNum;
    private AtomicLong count = new AtomicLong(0);
    private StringCodec stringCodec = new StringCodec();
    private long start = System.currentTimeMillis();

    Map<RedisURI, StatefulRedisConnection> statefulRedisConnectionMap = new ConcurrentHashMap<>();
    private Object o = new Object();

    public StatefulRedisConnection fetchStatefulRedisConnection(RedisURI redisURI) {
        if (statefulRedisConnectionMap.containsKey(redisURI)) {
            return statefulRedisConnectionMap.get(redisURI);
        } else {
            synchronized (o) {
                if (statefulRedisConnectionMap.containsKey(redisURI)) {
                    return statefulRedisConnectionMap.get(redisURI);
                }
                RedisClient redisClient = RedisClient.create(redisURI);
                StatefulRedisConnection<String, String> connect = redisClient.connect();
                statefulRedisConnectionMap.put(redisURI, connect);
                return connect;
            }
        }
    }

    public static void main(String[] args) {
        CacheTest cacheTest = new CacheTest();
        Scanner in = new Scanner(System.in);
        cacheTest.typeLevel = in.nextInt();
        cacheTest.keySize = in.nextInt();
        cacheTest.valueSize = in.nextInt();
        cacheTest.threadNum = in.nextInt();
        cacheTest.processThreadPool = new ThreadPoolExecutor(cacheTest.threadNum, cacheTest.threadNum,
                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.AbortPolicy());
        in.nextLine();
        String ip = in.nextLine();
        ArrayList<RedisURI> list = new ArrayList<>();
        list.add(RedisURI.create("redis://" + ip));


        RedisClusterClient client = RedisClusterClient.create(list);
        ClusterTopologyRefreshOptions options = ClusterTopologyRefreshOptions.builder()
                //开启集群拓扑结构周期性刷新，和默认参数保持一致
                .enablePeriodicRefresh(Duration.of(60, ChronoUnit.SECONDS))
                //开启针对{@link RefreshTrigger}中所有类型的事件的触发器
                .enableAllAdaptiveRefreshTriggers()
                //和默认一样，30s超时，避免短时间大量出现刷新拓扑的事件
                .adaptiveRefreshTriggersTimeout(Duration.of(30, ChronoUnit.SECONDS))
                //和默认一样先重连5次，然后再刷新集群拓扑
                .refreshTriggersReconnectAttempts(5)
                .build();
        client.setOptions(ClusterClientOptions
                .builder()
                .socketOptions(SocketOptions.builder().keepAlive(true).build())
                .autoReconnect(true)
                .topologyRefreshOptions(options)
                .build());
        StatefulRedisClusterConnection<String, String> connect = client.connect();
        cacheTest.connection = connect;
        cacheTest.client = client;
        cacheTest.asyncCommands = (RedisAdvancedClusterAsyncCommandsImpl) connect.async();
        cacheTest.beforeTest();
        cacheTest.test();
    }


    private void beforeTest() {
        String keyPre = "test:0:testmget";
        Set<String> keys = new HashSet<>();
        String value = "";
        for (int i = 0; i < valueSize; i++) {
            value += "a";
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keySize; i++) {
            String key = keyPre + i;
            keys.add(key);
            String nextValue = value + i;
            map.put(key, nextValue);
            try {
                asyncCommands.set(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void test() {
        Set<String> keys = new HashSet<>();
        String keyPre = "test:0:testmget";
        for (int i = 0; i < keySize; i++) {
            String key = keyPre + i;
            keys.add(key);
        }
        start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            processThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        dotest(keys);
                    }
                }
            });
        }
        processThreadPool.shutdown();
        try {
            processThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {

        }
    }

    public void dotest(Set<String> keys) {
        int size5 = 0;
        int size10 = 0;
        int size20 = 0;
        int size40 = 0;
        int size80 = 0;
        int size120 = 0;
        int size160 = 0;
        int size200 = 0;
        long start = System.nanoTime();
        int error = 0;
        for (int i = 0; i < 1000; i++) {
            long begin = System.currentTimeMillis();
            String[] keys1 = new String[keys.size()];
            int index = 0;
            for (String k : keys) {
                keys1[index] = k;
                index++;
            }
            try {
                if (typeLevel == 1) {
                    RedisFuture mget = asyncCommands.mget(keys);
                    Object o = mget.get();
                } else if (typeLevel == 2) {
                    List<KeyValue<String, String>> keyValues = doPipelineMget(keys);
                    int ds = 12;
                } else if (typeLevel == 3) {
                    CompletableFuture<List<KeyValue<String, String>>> future = doAsyncPipelineMget(keys);
                    List<KeyValue<String, String>> keyValues = future.get();
                    int ds = 12;
                }
//                else if (typeLevel == 4) {
//                    RedisFuture<List<KeyValue<String, String>>> future = doAsyncPipelineMget(keys);
//                    List<KeyValue<String, String>> keyValues = future.get();
//                    int ds = 12;
//                }

            } catch (Exception e) {
                e.printStackTrace();
                error++;
            }
            long end = System.currentTimeMillis();
            long cost = end - begin;
            if (cost >= 5) {
                size5++;
            }
            if (cost >= 10) {
                size10++;
            }
            if (cost >= 20) {
                size20++;
            }
            if (cost >= 40) {
                size40++;
            }
            if (cost >= 80) {
                size80++;
            }
            if (cost > 120) {
                size120++;
            }
            if (cost > 160) {
                size160++;
            }
            if (cost > 200) {
                size200++;
            }
            count.incrementAndGet();
        }
        long end = System.nanoTime();
        long l = System.currentTimeMillis();
        long qps = count.get() * 1000 / (l - this.start);
        StringBuffer buffer = new StringBuffer();
        buffer.append(Thread.currentThread().getName()).append(" ")
                .append("qps:").append(qps).append(" ")
                .append("cost:").append((end - start) / 1000 / 1000).append(" ")
                .append("size5:").append(size5).append(" ")
                .append("size10:").append(size10).append(" ")
                .append("size20:").append(size20).append(" ")
                .append("size40:").append(size40).append(" ")
                .append("size80:").append(size80).append(" ")
                .append("size120:").append(size120).append(" ")
                .append("size160:").append(size160).append(" ")
                .append("size200:").append(size200).append(" ")
                .append("error:").append(error).append(" ");
        System.out.println(buffer);
    }


//    public RedisFuture<List<KeyValue<String, String>>> doAsyncPipelineMget1(Set<String> keys) {
//        Map<Integer, List<String>> partitioned = SlotHash.partition(stringCodec, keys);
//        Map<String, Integer> slots = SlotHash.getSlots(partitioned);
//        Map<Integer, RedisFuture<List<KeyValue<String, String>>>> executions = new HashMap<>();
//        asyncPipelineMget(keys, executions);
//        PipelinedRedisFuture<List<KeyValue<String, String>>> pipelinedRedisFuture = new PipelinedRedisFuture<>(executions, objectPipelinedRedisFuture -> {
//            List<KeyValue<String, String>> ans = new ArrayList<>();
//            for (RedisFuture<List<KeyValue<String, String>>> future : executions.values()) {
//                ans.addAll(MultiNodeExecution.execute(() -> future.get()));
//            }
////            List<KeyValue<String, String>> result = new ArrayList<>();
////            for (String opKey : keys) {
////                int slot = slots.get(opKey);
////                int position = partitioned.get(slot).indexOf(opKey);
////                RedisFuture<List<KeyValue<String, String>>> listRedisFuture = executions.get(slot);
////                result.add(MultiNodeExecution.execute(() -> listRedisFuture.get().get(position)));
////            }
//            return ans;
//        });
//        return pipelinedRedisFuture;
//    }


    public List<AsyncCommand<String, String, List<KeyValue<String, String>>>> asyncPipelineMget(Set<String> keys, Map<Integer, RedisFuture<List<KeyValue<String, String>>>> executions) {
        Map<Integer, List<String>> partitioned = SlotHash.partition(stringCodec, keys);
        Partitions partitions = client.getPartitions();
        List<AsyncCommand<String, String, List<KeyValue<String, String>>>> commands = new LinkedList<>();
        Map<StatefulRedisConnection, List<ClusterCommand<String, String, List<KeyValue<String, String>>>>> map = new HashMap<>();
        RedisChannelWriter channelWriter = client.connect().getChannelWriter();
        for (Map.Entry<Integer, List<String>> entry : partitioned.entrySet()) {
            Integer slot = entry.getKey();
            RedisURI uri = partitions.getPartitionBySlot(slot).getUri();
            StatefulRedisConnection statefulRedisConnection = fetchStatefulRedisConnection(uri);

            CommandArgs<String, String> args = new CommandArgs<>(stringCodec).addKeys(entry.getValue());
            KeyValueListOutput<String, String> keyValueListOutput = new KeyValueListOutput<>(stringCodec, entry.getValue());
            Command<String, String, List<KeyValue<String, String>>> command = new Command<>(MGET, keyValueListOutput, args);
            AsyncCommand<String, String, List<KeyValue<String, String>>> asyncCommand = new AsyncCommand<>(command);
            ClusterCommand clusterCommand = new ClusterCommand(asyncCommand, channelWriter, 3);
            List<ClusterCommand<String, String, List<KeyValue<String, String>>>> list = map.getOrDefault(statefulRedisConnection, new LinkedList<>());
            list.add(clusterCommand);
            executions.put(slot, asyncCommand);
            map.put(statefulRedisConnection, list);
            commands.add(asyncCommand);
        }
        for (Map.Entry<StatefulRedisConnection, List<ClusterCommand<String, String, List<KeyValue<String, String>>>>> entry : map.entrySet()) {
            StatefulRedisConnection statefulRedisConnection = entry.getKey();
            statefulRedisConnection.dispatch(entry.getValue());
        }
        return commands;
    }

    public CompletableFuture<List<KeyValue<String, String>>> doAsyncPipelineMget(Set<String> keys) {
        Map<Integer, RedisFuture<List<KeyValue<String, String>>>> executions = new HashMap<>();
        List<AsyncCommand<String, String, List<KeyValue<String, String>>>> commands = asyncPipelineMget(keys, executions);
        CompletionStage<List<KeyValue<String, String>>> stage = CompletableFuture.completedFuture(new ArrayList<>());
        for (AsyncCommand<String, String, List<KeyValue<String, String>>> asyncCommand : commands) {
            stage = stage.thenCombine(asyncCommand, (result, resp) -> {
                result.addAll(resp);
                return result;
            });
        }
        CompletableFuture<List<KeyValue<String, String>>> future = stage.toCompletableFuture();
        return future;
    }


    public List<KeyValue<String, String>> doPipelineMget(Set<String> keys) {
        Map<Integer, RedisFuture<List<KeyValue<String, String>>>> executions = new HashMap<>();
        List<AsyncCommand<String, String, List<KeyValue<String, String>>>> commands = asyncPipelineMget(keys, executions);
        List<KeyValue<String, String>> ans = new ArrayList<>();
        for (AsyncCommand<String, String, List<KeyValue<String, String>>> future : commands) {
            List<KeyValue<String, String>> keyValues = null;
            try {
                keyValues = future.get();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            ans.addAll(keyValues);
        }
        return ans;
    }

    private void afterTest() {
        String keyPre = "test:0:testmget";
        Set<String> keys = new HashSet<>();
        String value = "";
        for (int i = 0; i < valueSize; i++) {
            value += "a";
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keySize; i++) {
            String key = keyPre + i;
            keys.add(key);
            String nextValue = value + i;
            map.put(key, nextValue);
            try {
                asyncCommands.del(key);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
