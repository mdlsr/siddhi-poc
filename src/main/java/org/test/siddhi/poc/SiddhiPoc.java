package org.test.siddhi.poc;

import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by nikolayloboda on 05/03/2018.
 */
public class SiddhiPoc {
    public static final String siddhiApp = builder()
            .append("define stream TestEventStream1 (Price float); ")
            .append("define stream TestEventStream2 (Price float); ")
            .append("@info(name = 'comparePrices') ")
            .append("from TestEventStream1#window.time(1 sec) as Stream1 ")
            .append("join TestEventStream2#window.time(1 sec) as Stream2 ")
            .append("on math:abs(Stream1.Price-Stream2.Price) > Stream1.Price*0.05  ")
            .append("select Stream1.Price as Price1, Stream2.Price as Price2 ")
            .append("insert into OutTestStream ;")
            .toString();

    public static void main(String... args) throws InterruptedException {
        SiddhiManager manage = new SiddhiManager();
        SiddhiAppRuntime runtime = manage.createSiddhiAppRuntime(siddhiApp);
        runtime.addCallback("OutTestStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event eachEvent : events) {
                    Object[] data = eachEvent.getData();
                    System.out.println(data[0] + " - " + data[1]);
                }
            }
        });

        InputHandler inputHandler1 = runtime.getInputHandler("TestEventStream1");
        InputHandler inputHandler2 = runtime.getInputHandler("TestEventStream2");
        runtime.start();

        while (!Thread.interrupted()) {
            inputHandler1.send(new Object[]{randomFloat(90, 110)});
            inputHandler2.send(new Object[]{randomFloat(90, 110)});
            Thread.sleep(100);
        }


    }

    private static final StringBuilder builder() {
        return new StringBuilder();
    }

    private static float randomFloat(float from, float to) {
        return (float) (from + (to - from) * Math.random());
    }
}
