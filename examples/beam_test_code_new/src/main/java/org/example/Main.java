package org.example;

import org.apache.arrow.flatbuf.Int;
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
        /*
        PCollection<KV<String, String>> pt1 = pipeline.apply(Create.of(KV.of("apple", "US"), KV.of("banana", "poland"), KV.of("apple", "china"), KV.of("orange", "mexico"), KV.of("banana", "ireland")));
        PCollection<KV<String, String>> pt2 = pipeline.apply(Create.of(KV.of("apple", "kashmir"), KV.of("banana", "maha"), KV.of("orange", "MP")));

        final TupleTag<String> t1 = new TupleTag<>();
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
                Map<String, ArrayList<String>> mp_int = new HashMap<String, ArrayList<String>>();
                Map<String, ArrayList<String>> mp_string = new HashMap<String, ArrayList<String>>();

                //System.out.println(e.getKey());
                String key = e.getKey();
                Iterable<String> allstrings = result.getAll(t1);
                for (String i : allstrings) {
                    if (mp_int.containsKey(key)) {
                        mp_int.get(key).add(i);
                    } else {
                        ArrayList<String> ad = new ArrayList<String>();
                        ad.add(i);
                        mp_int.put(key, ad);
                    }
                    //System.out.println(i);
                }
                //String string = e.getOnly(t2);
                Iterable<String> allString = result.getAll(t2);
                for (String s : allString) {
                    //System.out.println(s);
                    if (mp_string.containsKey(key)) {
                        mp_string.get(key).add(s);
                    } else {
                        ArrayList<String> ad = new ArrayList<String>();
                        ad.add(s);
                        mp_string.put(key, ad);
                    }
                }
                String output1 = result.getOnly(t2);
                //System.out.println("output:");
                //System.out.println(output1);
                Map<String, ArrayList<ArrayList<String>>> mp_final = new HashMap<String, ArrayList<ArrayList<String>>>();
                for (String key_ : mp_int.keySet()) {
                if (mp_final.containsKey(key_)) {
                    mp_final.get(key_).add(mp_int.get(key_));
                } else {
                    ArrayList<ArrayList<String>> ad = new ArrayList<ArrayList<String>>();
                    ad.add(mp_int.get(key_));
                    mp_final.put(key, ad);
                }
            }
                for (String key_ : mp_string.keySet()) {
                    if (mp_final.containsKey(key_)) {
                        mp_final.get(key_).add(mp_string.get(key_));
                    } else {
                        ArrayList<ArrayList<String>> ad = new ArrayList<ArrayList<String>>();
                        ad.add(mp_string.get(key_));
                        mp_final.put(key, ad);
                    }
                }
                //System.out.println(Collections.singletonList(mp_int));
                //System.out.println(Collections.singletonList(mp_string));
                System.out.println(Collections.singletonList(mp_final));
                c.output(output1);
            }}));*/

        ////cogroupbykey - end
        //Pardo - start
        /*PCollection<String> fruits = pipeline.apply(Create.of("apple", "banana", "orange", "avocado", "pineapple"));
        PCollection<String> op = fruits.apply(ParDo.of(new DoFn<String, String>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                String fruits_collection = c.element();
                String fruits = fruits_collection+"- it's a fruit";
                System.out.println(fruits);
                c.output(fruits);

            }}));*/

        //Pardo - end

        //Pardo input/output IO- start
       /* PCollection<String> fruits = pipeline.apply(TextIO.read().from("gs://beam_test_gr/input_text_files/df_input_int.txt"));
        PCollection<String> op = fruits.apply(ParDo.of(new DoFn<String, String>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                String numbers_collection = c.element();
                String[] arr = numbers_collection.split(",");
                String[] arr_new = new String[arr.length];
                int i = 0;
                for (String s : arr){
                    int val = Integer.parseInt(s.trim());
                    val += 3;
                    arr_new[i] = String.valueOf(val);
                    i+=1;

                }
                String array_str = String.join(",", arr_new);
                System.out.println(array_str);
                c.output(array_str);

            }}));
        op.apply(TextIO.write().to("gs://beam_test_gr/output/op.txt"));*/

        //Pardo input/output IO- end
        //group by key - start
        // PCollection<KV<String, String>> mapped = pipeline.apply(Create.of(KV.of("cat", "1"), KV.of("dog", "2"),
        //         KV.of("dog", "3"), KV.of("and", "4"), KV.of("and", "5")));
        // PCollection<KV<String, Iterable<String>>> reduced =
        //         mapped.apply(GroupByKey.<String, String>create());
        // PCollection<String> grp = reduced.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>,String>(){
        //     @ProcessElement
        //     public void processElement(ProcessContext c) {
        //         //KV<String, Iterable<String>> word = c.element();
        //         Map<String, ArrayList<String>> outputmap = new HashMap<String, ArrayList<String>>();
        //         String key = c.element().getKey();
        //         Iterable<String> val = c.element().getValue();
        //         String val_string = val.toString();
        //         System.out.println(key+","+val_string);
        //         c.output(key+","+val_string);
        //         //Integer length = word.length();

        //     }}));
        //group by key - end
        //cogroupbykey - start
        /*PCollection<KV<String, String>> pt1 = pipeline.apply(Create.of(KV.of("apple", "US"), KV.of("banana", "poland"),
                KV.of("apple", "china"), KV.of("orange", "mexico"), KV.of("banana", "ireland")));
        PCollection<KV<String, String>> pt2 = pipeline.apply(Create.of(KV.of("apple", "kashmir"),
                KV.of("banana", "maha"), KV.of("orange", "MP")));

        final TupleTag<String> t1 = new TupleTag<>();
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
                Map<String, ArrayList<String>> mp_int = new HashMap<String, ArrayList<String>>();
                Map<String, ArrayList<String>> mp_string = new HashMap<String, ArrayList<String>>();

                //System.out.println(e.getKey());
                String key = e.getKey();
                System.out.println("print Co/GBKOutput");
                System.out.println(key+","+result.toString());

                c.output(result.toString());
            }}));*/
        //cogroupbykey - end
        //combine - start
        /*PCollection<Integer> pc = pipeline.apply(Create.of(1, 8, 9, 10));
        PCollection<Integer> sum = pc.apply(
                Combine.globally(Sum.ofIntegers()));
        PCollection<Integer> op = sum.apply(ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Integer val = c.element();
                System.out.println(val);

                c.output(val);
            }}));*/
        //combine - end
//        //combine per key - start
//        PCollection<KV<String, Double>> salesRecords = pipeline.apply(Create.of(KV.of("abc", 2.6), KV.of("def", 8.9),
//                KV.of("abc", 9.8), KV.of("def", 9.9), KV.of("gbv", 6.7)));
//        //calculate sum by applying combine on the Pcollection
//        PCollection<KV<String, Double>> totalSalesPerPerson =
//                salesRecords.apply(Combine.<String, Double, Double>perKey(
//                        Sum.ofDoubles()));
//        //Print to see the results
//        PCollection<Double> op = totalSalesPerPerson.apply(ParDo.of(new DoFn<KV<String, Double>, Double>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                KV<String, Double> e = c.element();
//                String key = e.getKey();
//                Double val = e.getValue();
//                System.out.println(key+","+val);
//
//                c.output(e.getValue());
//            }}));
//
//        //combine per key - end

//        PCollection<String> pc1 = pipeline.apply(Create.of("apple", "banana", "orange"));
//        PCollection<String> pc2 = pipeline.apply(Create.of("avocado", "pineapple"));
//        PCollection<String> pc3 = pipeline.apply(Create.of("blackberry", "strawberry"));
//
//        PCollectionList<String> collections = PCollectionList.of(pc1).and(pc2).and(pc3);
//        PCollection<String> merged = collections.apply(Flatten.<String>pCollections());
//        PCollection<String> op = merged.apply(ParDo.of(new DoFn<String, String>(){
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                String fruits_collection = c.element();
//                System.out.println(fruits_collection);
//                c.output(fruits_collection);
//
//            }}));

        //side input

        // PCollection<String> fruits = pipeline.apply(Create.of("apple", "banana", "orange"));
        // PCollection<Integer> wordLengths = pipeline.apply(Create.of(3, 1, 5));
        // final PCollectionView<Integer> maxWordLengthCutOffView =
        //         wordLengths.apply(Combine.globally(Max.ofIntegers()).asSingletonView());

        // PCollection<String> wordsBelowCutOff =
        //         fruits.apply(ParDo
        //                 .of(new DoFn<String, String>() {
        //                     @ProcessElement
        //                     public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
        //                         // In our DoFn, access the side input.
        //                         int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
        //                         if (word.length() <= lengthCutOff) {
        //                             System.out.println(word);
        //                             out.output(word);
        //                         }
        //                     }
        //                 }).withSideInputs(maxWordLengthCutOffView)
        //         );
        // additional outputs -start
//        PCollection<String> words = pipeline.apply(Create.of("apple", "banana", "orange"));
//        final int wordLengthCutOff = 10;
//        final TupleTag<String> wordsBelowCutOffTag =   new TupleTag<String>(){};
//        final TupleTag<Integer> wordLengthsAboveCutOffTag =  new TupleTag<Integer>(){};
//        final TupleTag<String> markedWordsTag =   new TupleTag<String>(){};
//        PCollectionTuple results =  words.apply(ParDo
//                        .of(new DoFn<String, String>() {
//                            @ProcessElement
//                            public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
//                                if (word.length() > 5) {
//                                    System.out.println(word);
//                                    c.output(wordsBelowCutOffTag, word);
//                                }else {
//                                    c.output(markedWordsTag, word);
//                                }
//                                int length = word.length();
//                                if (length > 5) {
//                                    c.output(wordLengthsAboveCutOffTag, length);
//                                }
//                            }
//                        })
//                        .withOutputTags(wordsBelowCutOffTag,
//                                TupleTagList.of(wordLengthsAboveCutOffTag)
//                                        .and(markedWordsTag)));
//
//        PCollection<String> wordsaboveCutoff = results.get(wordsBelowCutOffTag);
//        PCollection<String> wordsBelowCutoff = results.get(wordsBelowCutOffTag);
//        PCollection<Integer> wordLengthCutoff = results.get(wordLengthsAboveCutOffTag);
//
//        wordsaboveCutoff.apply(ParDo.of(new DoFn<String, String>(){
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                String fruits_collection = c.element();
//                System.out.println("above cutoff fruits");
//                System.out.println(fruits_collection);
////                c.output(fruits_collection);
//
//            }}));

        // additional outputs - end
        //composite transforms - start
        PCollection<String> line = pipeline.apply(Create.of("apache beam is apache beam"));
        PCollection<KV<String, Long>> results =  line.apply(new CountWords());
        results.apply(ParDo.of(new DoFn<KV<String, Long>, String>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, Long> wordcount = c.element();
                System.out.println(wordcount.getKey() +"->"+wordcount.getValue());
            }}));




        pipeline.run();



}
    
    static class ExtractWordsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String lines = c.element();
            String arr[] = lines.split(" ");
            for (String element : arr){
                c.output(element);
            }


        }
    }
    public static class CountWords extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts =
                    words.apply(Count.<String>perElement());

            return wordCounts;
        }
    }
    }
