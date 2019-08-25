package scheduleexecutor;
import kafka_api.producermethod_sql;
import sql_connect.*;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class schedule_sql_call {
    public static void main(String[] args) throws InterruptedException {


        Runnable task1 = () -> {
            test_outout obj = new test_outout();
            ArrayList<String> s = obj.sqlstmt();

            for(int i =0; i < s.size();i++) {
                System.out.println((s.get(i)).toString());
                producermethod_sql.producestr(s.get(i).toString());
            }

            System.out.println("print1");
        };

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);


        ScheduledFuture<?> scheduledFuture = ses.scheduleAtFixedRate(task1, 3, 5, TimeUnit.SECONDS);


    }

}
