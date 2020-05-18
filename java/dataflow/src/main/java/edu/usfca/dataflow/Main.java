package edu.usfca.dataflow;


import java.nio.charset.Charset;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import edu.usfca.dataflow.transforms.ExtractData.ExtractAddicts;
import edu.usfca.dataflow.transforms.ExtractData.ExtractHighSpenders;
import edu.usfca.dataflow.transforms.PurchaserProfiles.GetProfilesFromEvents;
import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.PurchaserProfile;

/**
 */
public class Main {
  final private static String USER_EMAIL = "";

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  // TODO: Make sure you change the following to your own settings.
  final static String GCP_PROJECT_ID = "";
  final static String GCS_BUCKET = "";
  final static String GIT_ID = "";

  final static String INPUT_FILES = String.format("%s/project4-perf/data*.txt", GCS_BUCKET);
  final static String OUTPUT_ADDICT_FILE = String.format("%s/project4-output/addicts", GCS_BUCKET);
  final static String OUTPUT_SPENDER_FILE = String.format("%s/project4-output/spenders", GCS_BUCKET);
  final static String REGION = "us-west1";

  static DataflowPipelineOptions getOptions() {
    final String userDir = System.getProperty("user.dir");
    String jobName = "";
    // The following block attempts to figure out whether you are running pipeline from project 3 or project 4.
    // This is for convenience only (it has nothing to do with grading).
    if (userDir.contains("cs686-proj3-")) {
      jobName = "cs686-project3-data";
    } else if (userDir.contains("cs686-proj4-")) {
      jobName = "cs686-project4-data";
    } else {
      jobName = "cs686-unknown-data";
    }

    DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    // This will display the "current settings" stored in options.
    System.out.println(options.toString());

    options.setTempLocation(GCS_BUCKET + "/staging");
    options.setJobName(jobName);
    options.setRunner(DataflowRunner.class);// <- this line makes your pipeline run on GCP Dataflow.
    options.setMaxNumWorkers(1); // <- Let's use 1 VM worker (to save GCP credits) as this job requires little work.
    options.setWorkerMachineType("n1-standard-1"); // <- Standard machine with 1 vCPU.
    options.setDiskSizeGb(150); // <- Local Persistent Disk Size in GB.
    options.setRegion(REGION);
    options.setProject(GCP_PROJECT_ID);

    // You will see more info here.
    // To run a pipeline (job) on GCP via Dataflow, you need to specify a few things like the ones above.
    System.out.println(options.toString());
    return options;
  }

  static String getUserEmail() {
    return USER_EMAIL;
  }

  public static void main(String[] args) {
    System.out.println(Charset.defaultCharset());
    System.out.println(System.getProperty("file.encoding"));

    // TODO: When you are ready to run your pipeline on GCP, remove the following if statement.
    // Until then, I recommend you do not run it on GCP (so as not to waste GCP credits).
    if (2 + 3 != 5) {
      DataflowPipelineOptions options = getOptions();

      Pipeline p = Pipeline.create(options);

      // 1. From JSON logs to profiles.
      PCollection<PurchaserProfile> profiles = p//
          .apply("[cs686-s01]Read", TextIO.read().from(INPUT_FILES))//
          .apply("[cs686-s02]Parse", new GetProfilesFromEvents())//
          .apply("[cs686-s03]Merge", new MergeProfiles());

      // 2. Extract Addicts data, and write to GCS.
      profiles.apply("[cs686-s04]Addicts", new ExtractAddicts("id686486", 3)) //
          .apply("[cs686-s05]Map",
              MapElements.into(TypeDescriptors.strings())
                  .via((DeviceId id) -> String.format("%s:%s", id.getOs().name(), id.getUuid()))) //
          .apply("[cs686-s06]Write", TextIO.write().to(OUTPUT_ADDICT_FILE).withSuffix(".txt").withNumShards(1));

      // 3. Extract High Spenders data, and write to GCS.
      profiles
          .apply("[cs686-s07]Spender",
              new ExtractHighSpenders(3, 1000L,
                  new ImmutableSet.Builder<String>()
                      .add("id486", "id686", "id486686", "id686486", "dummy", "hey", "usf").build())) //
          .apply("[cs686-s08]Map",
              MapElements.into(TypeDescriptors.strings())
                  .via((DeviceId id) -> String.format("%s:%s", id.getOs().name(), id.getUuid()))) //
          .apply("[cs686-s09]Write", TextIO.write().to(OUTPUT_SPENDER_FILE).withSuffix(".txt").withNumShards(1));

      p.run().waitUntilFinish();
    }

    System.out.println("ta-da! All done!");
  }
}
