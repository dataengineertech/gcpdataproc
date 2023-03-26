package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.join.CoGbkResult;

import java.util.*;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        /**create the pipeline from pipeline options*/
        Pipeline pipeline = Pipeline.create(options);
        /*PCollection<String> allStrings = pipeline.apply(Create.of("hello", "world", "hi"));
        PCollection<String> longStrings = allStrings
                .apply(Filter.by(new SerializableFunction<String, Boolean>() {
                    @Override
                    public Boolean apply(String input) {
                        return input.length() > 3;
                    }
                }));
        longStrings.apply(TextIO.write().to("gs://beam_test_123/output/filter"));
        PCollection<String> lines = pipeline.apply(Create.of("Apache Beam is a new framework", "it is a new thing", "hi Beam"));
        PCollection<String> words = lines.apply(FlatMapElements.via(
                new InferableFunction<String, List<String>>() {
                    public List<String> apply(String line) throws Exception {
                        return Arrays.asList(line.split(" "));
                    }
                }));
        words.apply(TextIO.write().to("gs://beam_test_123/output/FlatMapElements"));*/
        /*PCollection<String> lines_new = pipeline.apply(Create.of("Apache Beam is a new framework", "it is a new thing", "hi Beam"));
        PCollection<String> extract_words = lines_new.apply("ExtractWords", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split(" "))));
        //word count
        PCollection<KV<String, Long>> word_count = extract_words.apply(Count.<String>perElement());
        word_count.apply("FormatResults", MapElements
                .into(TypeDescriptors.strings())
                .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue())).apply(TextIO.write().to("gs://beam_test_123/output/word_count"));
        //fetch keys
        PCollection<String> key_words = word_count.apply(Keys.<String>create());
        key_words.apply(TextIO.write().to("gs://beam_test_123/output/key_words"));*/

        //key swaps
        //PCollection<KV<Integer, String>> swap_word_count = word_count.apply(KvSwap.create());

        //add keys to value.. with keys
        /*PCollection<String> words = pipeline.apply(Create.of("Hello", "World", "Beam", "is", "fun"));
        PCollection<KV<Integer, String>> lengthAndWord =
                words.apply(WithKeys.of(new SerializableFunction<String, Integer>() {
                    @Override
                    public Integer apply(String s) {
                        return s.length();
                    }
                }));*/
       /* PCollection<String> lines = pipeline.apply(Create.of("Hello World1", "Beam is fun"));
        PCollection<String> lineLengths = lines.apply(MapElements.via(
                new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String line) {
                        LOG.info("elements in pcoll", line);
                        LOG.info("its length", String.valueOf(line.length()));
                        return String.valueOf(line.length());
                    }
                }));
        lineLengths.apply(TextIO.write().to("gs://beam_test_123/output/map"));*/

        //groupbykey -start
        /*PCollection<KV<String, String>> mapped = pipeline.apply(Create.of(KV.of("cat", "1"), KV.of("dog", "2"), KV.of("dog", "3"), KV.of("and", "4"), KV.of("and", "5")));
        PCollection<KV<String, Iterable<String>>> reduced =
                mapped.apply(GroupByKey.<String, String>create());
        PCollection<Map<String, ArrayList<String>>> grp = reduced.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>,Map<String, ArrayList<String>>>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                //KV<String, Iterable<String>> word = c.element();
                Map<String, ArrayList<String>> outputmap = new HashMap<String, ArrayList<String>>();
                String key = c.element().getKey();
                Iterable<String> val = c.element().getValue();
                System.out.println(key);
                for(String s : val){
                    System.out.println(s);
                    if (outputmap.containsKey(key)){
                        outputmap.get(key).add(s);
                    }
                    else{
                        ArrayList<String> valarr = new ArrayList<>();
                        valarr.add(s);
                        outputmap.put(key, valarr);
                    }
                }
                c.output(outputmap);
                //Integer length = word.length();

            }}));
        PCollection<String> op = grp.apply(ParDo.of(new DoFn<Map<String, ArrayList<String>>, String>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                Map<String, ArrayList<String>> outputmap1 = c.element();
                String text_file_op = "";
                for(Map.Entry<String, ArrayList<String>> entry: outputmap1.entrySet()) {
                    String key_val = entry.getKey() + " "+String.join(", ", entry.getValue());
                    text_file_op = text_file_op+"\n"+key_val;
                }
            c.output(text_file_op);

        }}));

        op.apply(TextIO.write().to("gs://beam_test_gr/output"));*/

        //groupbykey -end

        //cogroupbykey - start
        PCollection<KV<String, Integer>> pt1 = pipeline.apply(Create.of(KV.of("apple", 1), KV.of("banana", 2), KV.of("apple", 9), KV.of("orange", 7), KV.of("banana", 7)));
        PCollection<KV<String, String>> pt2 = pipeline.apply(Create.of(KV.of("apple", "kashmir"), KV.of("banana", "maha"), KV.of("orange", "MP")));

        final TupleTag<Integer> t1 = new TupleTag<>();
        final TupleTag<String> t2 = new TupleTag<>();
        PCollection<KV<String, CoGbkResult>> result =
                KeyedPCollectionTuple.of(t1, pt1).and(t2, pt2)
                        .apply(CoGroupByKey.create());
        PCollection<String> op = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, CoGbkResult> e = c.element();
                CoGbkResult result = e.getValue();
                // Retrieve all integers associated with this key from pt1
                Iterable<Integer> allIntegers = result.getAll(t1);
                for (int i : allIntegers){
                    System.out.println(i);
                }
                // Retrieve the string associated with this key from pt2.
                // Note: This will fail if multiple values had the same key in pt2.
                //String string = e.getOnly(t2);
                Iterable<String> allString = result.getAll(t2);
                for (String s: allString){
                    System.out.println(s);
                }
                c.output("hello");
            }}));
        //}


        pipeline.run();



}
    }