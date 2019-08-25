package scheduleexecutor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


public class scheduleexecutorclass {
    public static void main(String[] args) throws InterruptedException {


        Runnable task1 = () -> {

            System.out.println("Running...task1 - count : " );
        };

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);


        ScheduledFuture<?> scheduledFuture = ses.scheduleAtFixedRate(task1, 3, 3, TimeUnit.SECONDS);


    }

}
