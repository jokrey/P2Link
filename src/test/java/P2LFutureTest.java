import jokrey.utilities.debug_analysis_helper.AverageCallTimeMarker;
import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.util.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static jokrey.utilities.simple.data_structure.queue.ConcurrentQueueTest.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author jokrey
 */
public class P2LFutureTest {
    @Test
    void p2lFutureTest_get() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            sleep(100);
            p2lF.setCompleted(123);
            System.out.println("t2("+System.currentTimeMillis()+") completed");
        }).start();
        int x = p2lF.get(500);
        System.out.println("t1("+System.currentTimeMillis()+") - x = "+x);
        assertEquals(123, x);
    }
    @Test void p2lFutureTest_MULTIPLE_GET() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        p2lF.callMeBack(x -> System.out.println("t1 callback: x="+x));
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get(100);
            assertEquals(123, x);
            System.out.println("t2("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t3("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get(100);
            assertEquals(123, x);
            System.out.println("t3("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t4("+System.currentTimeMillis()+") init/waiting");
            int x = p2lF.get(100);
            assertEquals(123, x);
            System.out.println("t4("+System.currentTimeMillis()+") completed - x="+x);
        }).start();
        new Thread(() -> {
            System.out.println("t5("+System.currentTimeMillis()+") init/waiting");
            p2lF.callMeBack(x -> System.out.println("t5 callback: x="+x));
            System.out.println("t5("+System.currentTimeMillis()+") completed");
        }).start();
        new Thread(() -> {
            System.out.println("t6("+System.currentTimeMillis()+") init/waiting");
            p2lF.callMeBack(x -> System.out.println("t6 callback: x="+x));
            System.out.println("t6("+System.currentTimeMillis()+") completed");
        }).start();

        p2lF.setCompleted(123);
        int x = p2lF.get(1);
        assertEquals(123, x);
        System.out.println("t1("+System.currentTimeMillis()+") - x = "+x);
        sleep(1000);
    }
    @Test void p2lFutureTest_callMeBack() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            sleep(100);
            p2lF.setCompleted(123);
            System.out.println("t2("+System.currentTimeMillis()+") completed");
        }).start();

        p2lF.callMeBack(x -> System.out.println("t1("+System.currentTimeMillis()+") - x = "+x));

        sleep(250); // without this the thread is killed automatically when this method returns
        System.out.println("t1 completed");
    }
    @Test void p2lFutureTest_timeout() {
        System.out.println("t1("+System.currentTimeMillis()+") init");

        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> {
            System.out.println("t2("+System.currentTimeMillis()+") init/waiting");
            sleep(1000);
            p2lF.setCompleted(123);
            System.out.println("t2("+System.currentTimeMillis()+") completed");
        }).start();

        System.out.println("t1("+System.currentTimeMillis()+") starting wait");
        assertThrows(TimeoutException.class, () -> p2lF.get(100));
        System.out.println("t1("+System.currentTimeMillis()+") completed");
    }
    @Test void p2lFutureTest_combine() {
        P2LFuture<Integer> p2lF1 = new P2LFuture<>();
        P2LFuture<Integer> p2lF2 = new P2LFuture<>();
        P2LFuture<Integer> combine = p2lF1.combine(p2lF2, P2LFuture.PLUS);
        new Thread(() -> {
            p2lF1.setCompleted(1);
            sleep(1000);
            p2lF2.setCompleted(3);
        }).start();

        assertThrows(TimeoutException.class, () -> combine.get(100));
        assertEquals(new Integer(1), p2lF1.get(500));
        assertEquals(new Integer(3), p2lF2.get(1500));
        assertEquals(new Integer(4), combine.get(1500));
    }
    @Test void p2lFutureTest_cancelCombine() {
        P2LFuture<Integer> p2lF1 = new P2LFuture<>();
        P2LFuture<Integer> p2lF2 = new P2LFuture<>();
        P2LFuture<Integer> combine = p2lF1.combine(p2lF2, P2LFuture.PLUS);
        new Thread(() -> {
            p2lF1.setCompleted(1);
            sleep(1000);
            p2lF2.trySetCompleted(3); //supposed to fail
        }).start();

        assertThrows(TimeoutException.class, () -> combine.get(100));
        assertEquals(new Integer(1), p2lF1.get(500));
        assertThrows(AlreadyCompletedException.class, p2lF1::cancel);
        p2lF2.cancel();
        assertThrows(CanceledException.class, combine::get);
        assertThrows(CanceledException.class, p2lF2::get);
    }
    @Test void p2lFutureTest_cannotCompleteTwice() {
        P2LFuture<Integer> p2lF = new P2LFuture<>();
        new Thread(() -> p2lF.setCompleted(1)).start();
        sleep(500);

        assertEquals(new Integer(1), p2lF.get(500));
        assertThrows(AlreadyCompletedException.class, () -> p2lF.setCompleted(3));
        assertEquals(new Integer(1), p2lF.get(500));
    }
    @Test void p2lFutureTest_reducePerformanceComparison() {
        int iterations = 1000000;

        reduceUsingCombine(iterations);
        reduceUsingWhenCompleted(iterations);

        for(int repeatCounter=0;repeatCounter<33;repeatCounter++) {
            AverageCallTimeMarker.mark_call_start("reduce using combine");
            reduceUsingCombine(iterations);
            AverageCallTimeMarker.mark_call_end("reduce using combine");

            AverageCallTimeMarker.mark_call_start("reduce using when completed");
            reduceUsingWhenCompleted(iterations);
            AverageCallTimeMarker.mark_call_end("reduce using when completed");
        }
        AverageCallTimeMarker.print_all();
        AverageCallTimeMarker.clear();
    }
    private static void reduceUsingCombine(int iterations) {
        List<P2LFuture<Integer>> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++)
            list.add(new P2LFuture<>(new Integer(1)));

        P2LFuture<Integer> reduced = P2LFuture.reduce(list, P2LFuture.PLUS);
        assertEquals(iterations, reduced.get().intValue());
    }
    private static void reduceUsingWhenCompleted(int iterations) {
        List<P2LFuture<Integer>> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++)
            list.add(new P2LFuture<>(new Integer(1)));

        P2LFuture<Integer> reduced = P2LFuture.reduceWhenCompleted(list, P2LFuture.PLUS);
        assertEquals(iterations, reduced.get().intValue());
    }


    @Test void p2lThreadPoolTest() {
        TimeDiffMarker.setMark(5331);
        {
            ArrayList<P2LFuture<Boolean>> allFs = new ArrayList<>(100);
            P2LThreadPool pool = new P2LThreadPool(2, 32);
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < 100; i++)
                allFs.add(pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                }));
            System.out.println("x1");
            assertTrue(P2LFuture.reduce(allFs, P2LFuture.AND).get());
            System.out.println("x2");
            assertEquals(counter.get(), 100);
            pool.shutdown();
        }
        TimeDiffMarker.println(5331, "custom 1 took: ");


        TimeDiffMarker.setMark(5331);
        {
            P2LThreadPool pool = new P2LThreadPool(2, 32);
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < 100; i++)
                pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                });
            while (counter.get() < 100) sleep(10);
            assertEquals(counter.get(), 100);
            pool.shutdown();
        }
        TimeDiffMarker.println(5331, "custom 2 took: ");

        TimeDiffMarker.setMark(5331);
        {
            ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 32, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < 100; i++)
                pool.execute(() -> {
                    sleep(100);
                    counter.getAndIncrement();
                });
            while (counter.get() < 100) sleep(10);
            assertEquals(counter.get(), 100);
        }
        TimeDiffMarker.println(5331, "custom took: ");
    }
    @Test void p2lThreadPoolTest_capacity() {
        P2LThreadPool pool = new P2LThreadPool(1, 1, 1);
        AtomicInteger counter = new AtomicInteger(1);
        pool.execute(() -> {
            sleep(200);
            counter.getAndIncrement();
        });
        pool.execute(() -> {
            sleep(200);
            counter.getAndIncrement();
        });
        assertThrows(CapacityReachedException.class, () -> pool.execute(() -> {
            sleep(200);
            counter.getAndIncrement();
        }));
        while (counter.get() < 2) sleep(10);
        assertEquals(counter.get(), 2);
    }

    @Test void p2lThreadPoolTest_manyTasks() {
        int num = 500000;
        int coreThreads = 1000;
        int maxThreads = 1000;

        AtomicInteger counter = new AtomicInteger(0);
        List<P2LThreadPool.ProvidingTask<Integer>> tasks = new ArrayList<>(num);
//        CopyOnWriteArrayList<Integer> list = new CopyOnWriteArrayList<>();
        for(int i = 0; i<num;i++) {
            int i_final = i;
            P2LThreadPool.ProvidingTask<Integer> task = () -> {
//                System.out.println("in task: "+i_final+" ");
//                list.add(i_final);
                return counter.getAndIncrement();
            };
            tasks.add(task);
        }

        P2LThreadPool pool = new P2LThreadPool(coreThreads, maxThreads);
        List<P2LTask<Integer>> x = pool.execute(tasks);

//        sleep(3000);
//        for(int i = 0; i<num;i++) {
//            if(!list.contains(i))
//                System.out.println("task("+i+") not executed");
//        }

        P2LFuture.oneForAll(x).get();
        assertEquals(num, counter.get());
    }


    @Test public void p2lAsyncTimeoutTest() {
        {
            System.out.println("t1(" + System.currentTimeMillis() + ") init");

            P2LFuture<Integer> p2lF = new P2LFuture<>();
            new Thread(() -> {
                System.out.println("t2(" + System.currentTimeMillis() + ") init/waiting");
                sleep(100);
                p2lF.setCompleted(123);
                System.out.println("t2(" + System.currentTimeMillis() + ") completed");
            }).start();

            P2LFuture<Boolean> bool = new P2LFuture<>();
            p2lF.callMeBack(500, integer -> {
                bool.setCompleted(true);
            });
            bool.waitForIt(5000);
            int x = p2lF.getResult();
            System.out.println("t1(" + System.currentTimeMillis() + ") - x = " + x);
            assertEquals(123, x);
        }

        {
            P2LFuture<Integer> p2lF = new P2LFuture<>();
            new Thread(() -> {
                System.out.println("t2(" + System.currentTimeMillis() + ") init/waiting");
                sleep(1000);
                p2lF.trySetCompleted(123);
                System.out.println("t2(" + System.currentTimeMillis() + ") completed");
            }).start();

            P2LFuture<Boolean> bool = new P2LFuture<>();
            p2lF.callMeBack(500, integer -> {
                bool.setCompleted(true);
            });
            bool.waitForIt(); //DOES NOT COMPLETE EVER, IF IT DOES NOT WORK...
            Integer x = p2lF.getResult();
            System.out.println("t1(" + System.currentTimeMillis() + ") - x = " + x);
            assertNull(p2lF.getResult());
        }

        {
            P2LFuture<Integer> p2lF_0 = new P2LFuture<>();
            P2LFuture<Integer> p2lF_1 = new P2LFuture<>();
            P2LFuture<Integer> p2lF_2 = new P2LFuture<>();
            P2LFuture<Integer> p2lF_3 = new P2LFuture<>();
            new Thread(() -> {
                System.out.println("t2(" + System.currentTimeMillis() + ") init/waiting");
                sleep(1000);
                p2lF_0.trySetCompleted(123);
                p2lF_1.trySetCompleted(123);
                p2lF_2.trySetCompleted(123);
                p2lF_3.trySetCompleted(123);
                System.out.println("t2(" + System.currentTimeMillis() + ") completed");
            }).start();

            AtomicInteger counter = new AtomicInteger();
            p2lF_3.callMeBack(1500, integer -> {
                if(integer != null) counter.getAndIncrement();
            });
            p2lF_0.callMeBack(10, integer -> {
                if(integer != null) counter.getAndIncrement();
            });
            p2lF_2.callMeBack(1200, integer -> {
                if(integer != null) counter.getAndIncrement();
            });
            p2lF_1.callMeBack(500, integer -> {
                if(integer != null) counter.getAndIncrement();
            });

            sleep(2000);
            assertEquals(2, counter.get());
            assertNull(p2lF_0.getResult());
            assertNull(p2lF_1.getResult());
            assertEquals(123, p2lF_2.getResult().intValue());
            assertEquals(123, p2lF_3.getResult().intValue());
        }

        AsyncTimeoutSchedulerThread.shutdown();//optional
    }
}
