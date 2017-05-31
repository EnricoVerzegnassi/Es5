package it.uniroma1.dis.bdc.stream;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.flink.streaming.api.windowing.windows.Window;


public class streaming2 {
    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the execution of the apache flink engine.
     */
    public static void main(final String[] args) throws Exception {


        // The ExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //Understands that we work with event's time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> rawdata2 = env.readTextFile("/home/verz/Temporanea/DaCancellare/Esercizio5Data/2005/2005.csv");

        DataStream<Tuple3<String, String, Integer>> text = rawdata2  //Splitta la linea nei diversi campi
                .flatMap(new Splitter());

        DataStream<Tuple3<String, String, Integer>> marked =        //asseno i timestamp
                text.assignTimestampsAndWatermarks(new myWatermark());


        DataStream<Tuple3<String, String, Integer>> grouped =
                marked
                        .keyBy(1)
                        .window(TumblingEventTimeWindows.of(Time.days(7L)))
                        .apply(new WindowFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Tuple, TimeWindow>() {
                            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, String, Integer>> iterable,
                                              Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                                int sum = 0;
                                String country ="";
                                String date = "";
                                for(Tuple3<String, String, Integer> t  : iterable) {
                                    sum += t.f2;
                                    country = t.f1;
                                    date = t.f0;
                                }
                                Calendar cal = Calendar.getInstance();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                                Date d = sdf.parse(date);
                                cal.setTime(d);
                                int week = cal.get(Calendar.WEEK_OF_YEAR);

                                collector.collect(new Tuple3<String, String, Integer>(String.valueOf(week),country,sum));
                            }
                        });
        //grouped.print();


//(WEEK,COUNTRY,NEVENTS)



            grouped
                    .keyBy(0)

                    .fold(new ArrayList<Tuple3<String,String,Integer>>(),
                            new FoldFunction<Tuple3<String,String,Integer>,
                                    ArrayList<Tuple3<String,String,Integer>>>() {
                        public ArrayList<Tuple3<String,String,Integer>> fold(ArrayList<Tuple3<String,String,Integer>> current,
                                                                             Tuple3<String,String,Integer> value) {
                            if(current.size()<5){
                                current.add(value);
                                return current;
                            }
                            else {

                                Tuple3<String,String,Integer> min = current.get(0);
                                for(Tuple3<String, String, Integer> tupla : current){
                                    if(tupla.f2 < min.f2){
                                        min = tupla;
                                    }
                                }
                                if(value.f2>min.f2){
                                    current.remove(min);
                                    current.add(value);
                                }

                                Collections.sort(current, new mioComparatoreReverse() );
                                return current;
                            }
                        }
                    })
                    .print().setParallelism(1);

        env.execute();
    }

    private static class  mioComparatoreReverse implements Comparator<Tuple3<String,String, Integer>> {

        public int compare(Tuple3<String,String, Integer> t1, Tuple3<String, String, Integer> t2) {
            return t2.f2 - t1.f2;
        }
    }

    //Ci pensiamo dopo cit.
    public static class myWatermark implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Integer>>{

        private static final long serialVersionUID = 1L;
        private long maxTimestamp = 0;
        private long priorTimestamp = 0;
        private final long maxDelay = 86400000; //1 day
        //private final long maxDelay = 604800000; //1 week
        //private final long maxDelay = 1000; //1 s


        public Watermark getCurrentWatermark() {
            System.err.println("WM="+this.maxTimestamp);
            return new Watermark(this.maxTimestamp);
        }


        public long extractTimestamp(Tuple3<String, String, Integer> mytuple, long priorTimestamp) {
            Calendar calendar = GregorianCalendar.getInstance();
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            Date date = null;
            try {
                date = format.parse(mytuple.f0);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            calendar.setTime(date);
            maxTimestamp = Math.max(calendar.getTimeInMillis(), priorTimestamp);
            return calendar.getTimeInMillis();
        }
    }


    public static class Splitter implements FlatMapFunction<String, Tuple3<String, String, Integer>> {

        public void flatMap(String sentence, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String[] words = sentence.split("\t");
            String date = words[1];
            String country = words[44]; //CountryID, Full Name:43
            int numEvents = 1;
            if(country.length()!=0) {
                out.collect(new Tuple3<String, String, Integer>(date, country, numEvents));
            }
        }
    }

}

