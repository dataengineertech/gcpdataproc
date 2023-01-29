package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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
        PCollection<String> lines = pipeline.apply(Create.of("Hello World1", "Beam is fun"));
        PCollection<String> lineLengths = lines.apply(MapElements.via(
                new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String line) {
                        LOG.info("elements in pcoll", line);
                        LOG.info("its length", String.valueOf(line.length()));
                        return String.valueOf(line.length());
                    }
                }));
        lineLengths.apply(TextIO.write().to("gs://beam_test_123/output/map"));

        pipeline.run();


    }
}