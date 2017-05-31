package it.uniroma1.dis.bdc.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * A simple example of Apache Flink batch processing engine using the Sacramento Police Department open dataset.
 * We here count the number of occurrences per city district.
 *
 * @author ichatz@gmail.com
 */
public final class CrimeDistrict {

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
                filename = "/tmp/crime.csv";
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
        DataSet<Tuple1<String>> rawdata = env.readCsvFile(filename)
                .includeFields("001")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class);

        // employ a simple map-reduce to count the number of rows per district
        DataSet<Tuple2<String, Integer>> result = rawdata
                .flatMap(new Counter()) //map the data and as district,1
                .groupBy(0) // group the data according to district
                .sum(1); // to count no. of crimes in a district

        result.print();
    }

    /**
     * Simple class to translate the input line (from the crime data set) into tuples (district-id, 1).
     */
    private final static class Counter
            implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {

        /**
         * Main method used by the apache flink flatMap method.
         *
         * @param value - the one-value Tuple (district-id) as read from the csv file.
         * @param out   - the two-value tuple (district-id, 1).
         */
        public void flatMap(final Tuple1<String> value, final Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<String, Integer>(value.f0, 1));
        }
    }


}
