import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.FailsafeValueInSingleWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PubsubtoBigquery {
    public static void main(String[] args) {
        final String subscription1 = "projects/hardy-position-352014/subscriptions/orders-stream-sub";
        final String subscription2 = "projects/hardy-position-352014/subscriptions/orders-others-sub";
        final String tablespec = "hardy-position-352014.ecommerce.orders-streams-others";
        /** create PipelineOptions from the args */
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        /**create the pipeline from pipeline options*/
        Pipeline pipeline = Pipeline.create(options);

        /** read data from both subscriptions with fixed window of 5 secs */
        PCollection<String> message_source1 = pipeline.apply(PubsubIO.<String>readStrings().fromSubscription(subscription1))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))).discardingFiredPanes());
        PCollection<String> message_source2 = pipeline.apply(PubsubIO.<String>readStrings().fromSubscription(subscription2))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))).discardingFiredPanes());

        /** merge both the sources*/

        PCollectionList<String> list_collections = PCollectionList.of(message_source1).and(message_source2);
        PCollection<String> merged_source = list_collections.apply(Flatten.<String>pCollections());

        /** convert Pcollection to TableRows */
        PCollection<TableRow> tableroescollections = merged_source.apply(ParDo.of(new ConvertJsonStringToTableRows()));

        /**write output to BQ table*/
        tableroescollections.apply(BigQueryIO.writeTableRows().to(tablespec)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    }


        public static class ConvertJsonStringToTableRows extends DoFn<String, TableRow>{
            @DoFn.ProcessElement
            public void processing (ProcessContext processContext)  {
                TableRow tableRow = new TableRow();
                try {
                    String jsonString = processContext.element();

                    byte[] messages_bytes = jsonString.getBytes(StandardCharsets.UTF_8);
                    InputStream inputStream = new ByteArrayInputStream(messages_bytes);
                    tableRow = TableRowJsonCoder.of().decode(inputStream);

                }
                catch (Exception e){
                    e.printStackTrace();
                }
                processContext.output(tableRow);

            }




    }
}
