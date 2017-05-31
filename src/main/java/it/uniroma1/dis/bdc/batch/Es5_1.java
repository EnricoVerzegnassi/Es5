package it.uniroma1.dis.bdc.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

/**
 * A simple example of Apache Flink batch processing engine using the Sacramento Police Department open dataset.
 * We here count the number of occurrences per city district.
 *
 * @author ichatz@gmail.com
 */
public final class Es5_1 {

    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the execution of the apache flink engine.
     */
    public static void main(final String[] args) throws Exception {

        // the filename to use as input dataset
        final String filename;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/home/verz/Temporanea/DaCancellare/Esercizio5Data/GDELT.MASTERREDUCEDV2.1979-2013/GDELT.MASTERREDUCEDV2_ridotto.txt";
                System.err.println("No filename specified. Please run 'CrimeDistrict " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'CrimeDistrict " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        // The ExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create a dataset based on the csv file based only on the 6th column containing the district ID
        DataSet<Tuple3<String, String, Integer>> rawdata =
                env.readCsvFile(filename)
                        .includeFields("11001")
                        .ignoreFirstLine()
                        .fieldDelimiter("\t")
                        .parseQuotedStrings('"')
                        .types(String.class, String.class, Integer.class);

        rawdata.print();

        //Rawdata <(data, paese), numevents>
        DataSet<Tuple2<Tuple2<String, String>, Integer>> first = rawdata
//                .flatMap() //map the data and as district,1
                .groupBy(0, 1) // group the data according to district
                .reduceGroup(new reduceToCouple()); // to count no. of crimes in a district
//
        first.print();

        DataSet<Tuple2<String, ArrayList<Tuple2<String, Integer>>>> second = first.map(new MapFunction<Tuple2<Tuple2<String, String>, Integer>
                , Tuple2<String, Tuple2<String, Integer>>>() {

            public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<Tuple2<String, String>, Integer> value) throws Exception {
                Tuple2<String, String> tupla = value.f0;
                String data = tupla.f0;
                String paese = tupla.f1;
                Integer eventi = value.f1;

                return new Tuple2<String, Tuple2<String, Integer>>(data, new Tuple2<String, Integer>(paese, eventi));
            }
        }).groupBy(0).reduceGroup(new reduceToTop5());
//
        second.print();
    }

    /**
     * Simple class to reduce the input line (from the crime data set) into tuples (offense code, count).
     * Remark that since the input file includes numbers within double-quotes we are loading the data using a String
     * data type and then converting it to Integer.
     */
    private final static class reduceToCouple
            implements GroupReduceFunction<Tuple3<String, String, Integer>, Tuple2<Tuple2<String, String>, Integer>> {

        /**
         * The method that defines the way the tuples are reduced.
         *
         * @param records - the records corresponding to this key.
         * @param out     - the tuple with the simple count of elements.
         * @throws Exception - in case any exception occurs during the conversion of the offense code into Integer.
         */
        public void reduce(Iterable<Tuple3<String, String, Integer>> records, Collector<Tuple2<Tuple2<String, String>, Integer>> out) throws Exception {

            //String offense = null;
            int cnt = 0;
            String key = null;
            String key2 = null;

            // iterate through the set of tuples
            for (Tuple3<String, String, Integer> numEvents : records) {
                // count the number of occurrence
                key = numEvents.f0; //data
                key2 = numEvents.f1; //paese
                cnt = cnt + numEvents.f2;
            }

            // emit offense code and count
            out.collect(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(key, key2), cnt));
        }
    }

    private final static class reduceToTop5
            implements GroupReduceFunction<Tuple2<String, Tuple2<String, Integer>>,
            Tuple2<String, ArrayList<Tuple2<String, Integer>>>> {

        /**
         * The method that defines the way the tuples are reduced.
         *
         * @param records - the records corresponding to this key.
         * @param out     - the tuple with the simple count of elements.
         * @throws Exception - in case any exception occurs during the conversion of the offense code into Integer.
         */
        public void reduce(Iterable<Tuple2<String, Tuple2<String, Integer>>> records,
                           Collector<Tuple2<String, ArrayList<Tuple2<String, Integer>>>> out) throws Exception {

            //String offense = null;
            int cnt = 0;
            String key = null;
            ArrayList<Tuple2<String, Integer>> top5 = new ArrayList<Tuple2<String, Integer>>();

            int min=0;
            for (Tuple2<String, Tuple2<String, Integer>> numEvents : records){
                key=numEvents.f0;
                top5.add(numEvents.f1);
            }

            Collections.sort(top5, new mioComparatoreReverse());

            ArrayList<Tuple2<String, Integer>> arrayout = new ArrayList<Tuple2<String, Integer>>(top5.subList(0, 5));

            // emit offense code and count
            out.collect(new Tuple2<String, ArrayList<Tuple2<String,Integer>>>(key, arrayout));
        }
    }

    private static class  mioComparatoreReverse implements Comparator<Tuple2<String, Integer>> {

        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t2.f1 - t1.f1;
        }
    }
}