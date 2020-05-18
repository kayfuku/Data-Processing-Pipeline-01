package edu.usfca.dataflow.transforms;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.transforms.ExtractData.ExtractAddicts;
import edu.usfca.dataflow.transforms.ExtractData.ExtractHighSpenders;
import edu.usfca.dataflow.transforms.ExtractData.ExtractInAppPurchaseData;
import edu.usfca.dataflow.transforms.PurchaserProfiles.GetProfilesFromEvents;
import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.dataflow.utils.CommonUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static edu.usfca.dataflow.transforms.Utils.getCanonicalDeviceId;
import static edu.usfca.dataflow.transforms.Utils.getMultiSet;
import static org.junit.Assert.assertEquals;

public class TestsWith03MediumDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 40 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  // @Rule
  // public Timeout timeout = Timeout.millis(40000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  final static String PATH_TO_FILE = "../judge/resources/sample-medium.txt";

  @Test
  public void test() {
    // this is a dummy test. freebies!
  }

  @Test
  public void __shareable__03medium_01common() {
    PCollection<PurchaserProfile> actualPp =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents());

    {

      PAssert.that(actualPp).satisfies(out -> {

        assertEquals(5000, Iterables.size(out));
        assertEquals(5000,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 1).count());

        return null;
      });
    }

    PCollection<PurchaserProfile> actualMerged = actualPp.apply(new MergeProfiles());

    {
      PAssert.that(actualMerged).satisfies(out -> {

        assertEquals(1362, Iterables.size(out));

        assertEquals(355,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 2).count());
        assertEquals(332,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 3).count());
        assertEquals(250,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() > 4).count());

        return null;
      });
    }
    tp.run();
  }

  @Test
  public void __shareable__03medium_02IAPP() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected = Arrays
        .asList("CghpZDY4NjQ4NhCYAxi4vocC", "CgbsuZjtgqgQ0wEYpq1T", "CgVpZDY4NhD7AhiA7IQC",
            "CgphcHAu652866m0EJIDGJmK5QE=", "CgVpZDQ4NhCHAxiYst8B", "CghpZDQ4NjY4NhDmAhiin84B",
            "Cgbsu6TtlLwQ4AEYlOCUAQ==", "CgphcHAu64OJ66m0EIsDGNHa4AE=", "CgbtlLzsnpAQ5QEYtOBm",
            "Cg1hcHAu67mE67mU66m0EOwCGMn94wE=", "CgbliqDmsrkQ1QEYoqlw", "Cg1hcHAu7Kec7J6l66m0EIwDGPug9QE=")
        .stream().map(b64 -> {
          try {
            return ProtoUtils.decodeMessageBase64(InAppPurchaseProfile.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        }).collect(Collectors.toSet());

    PAssert.that(actualMerged.apply(new ExtractInAppPurchaseData())).satisfies(out -> {
      Set<InAppPurchaseProfile> actual = StreamSupport.stream(out.spliterator(), false).collect(Collectors.toSet());

      assertEquals(expected, actual);
      return null;
    });

    tp.run();
  }

  @Test
  public void __shareable__03medium_02HighSpender() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDM2OUVCNjNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEY2MTJGQkZELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDAwQjYxNkY4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(4, 1000L, new ImmutableSet.Builder<String>().add("id486").build())))
          .satisfies(out -> {

            ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
                .addAll(StreamSupport.stream(out.spliterator(), false)
                    .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                .build();

            assertEquals(expectedIds, actualIds);
            return null;
          });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDVGNEEzN0JBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged.apply(new ExtractHighSpenders(5, 12345L,
              new ImmutableSet.Builder<String>()
                  .add("id686", "id486", "id486686", "id686486", "this", "does", "not", "exist").build())))
          .satisfies(out -> {

            ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
                .addAll(StreamSupport.stream(out.spliterator(), false)
                    .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                .build();

            assertEquals(expectedIds, actualIds);
            return null;
          });
    }

    tp.run();
  }

  @Test  // Failed.
  public void __shareable__03medium_02Addicts() {
    PCollection<PurchaserProfile> actualMerged = tp
        .apply(TextIO.read().from(PATH_TO_FILE))
        .apply(new GetProfilesFromEvents())
        .apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDEzQjUzNjY3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged.apply(new ExtractAddicts("id686486", 3))).satisfies(out -> {
        ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    tp.run();
  }

  // ----------

  @Test
  public void __shareable_all() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected = Arrays
        .asList("CghpZDY4NjQ4NhCYAxi4vocC", "CgbsuZjtgqgQ0wEYpq1T", "CgVpZDY4NhD7AhiA7IQC",
            "CgphcHAu652866m0EJIDGJmK5QE=", "CgVpZDQ4NhCHAxiYst8B", "CghpZDQ4NjY4NhDmAhiin84B",
            "Cgbsu6TtlLwQ4AEYlOCUAQ==", "CgphcHAu64OJ66m0EIsDGNHa4AE=", "CgbtlLzsnpAQ5QEYtOBm",
            "Cg1hcHAu67mE67mU66m0EOwCGMn94wE=", "CgbliqDmsrkQ1QEYoqlw", "Cg1hcHAu7Kec7J6l66m0EIwDGPug9QE=")
        .stream().map(b64 -> {
          try {
            return ProtoUtils.decodeMessageBase64(InAppPurchaseProfile.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        }).collect(Collectors.toSet());

    PAssert.that(actualMerged.apply(new ExtractInAppPurchaseData())).satisfies(out -> {
      Set<InAppPurchaseProfile> actual = StreamSupport.stream(out.spliterator(), false).collect(Collectors.toSet());
      assertEquals(expected, actual);
      return null;
    });


    {
      Multiset<String> expectedIds = getMultiSet("CAESJDM2OUVCNjNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEY2MTJGQkZELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDAwQjYxNkY4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(4, 1000L, new ImmutableSet.Builder<String>().add("id486").build())))
          .satisfies(out -> {

            ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
                .addAll(StreamSupport.stream(out.spliterator(), false)
                    .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                .build();

            assertEquals(expectedIds, actualIds);
            return null;
          });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDVGNEEzN0JBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged.apply(new ExtractHighSpenders(5, 12345L,
              new ImmutableSet.Builder<String>()
                  .add("id686", "id486", "id486686", "id686486", "this", "does", "not", "exist").build())))
          .satisfies(out -> {

            ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
                .addAll(StreamSupport.stream(out.spliterator(), false)
                    .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                .build();

            assertEquals(expectedIds, actualIds);
            return null;
          });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDEzQjUzNjY3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged.apply(new ExtractAddicts("id686486", 3))).satisfies(out -> {
        ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    tp.run();
  }



    final static String PATH_TO_FILE2 = "../judge/resources/sample-tiny-for-debug.txt";
    final static String PATH_TO_FILE_SMALL = "../judge/resources/sample-small.txt";

    @Test
    public void myTest00() {

        PCollection<PurchaserProfile> actualMerged = tp
            .apply(TextIO.read().from(PATH_TO_FILE))
            .apply(new GetProfilesFromEvents())
            .apply(new MergeProfiles());

//        actualMerged.apply(new CommonUtils.StrPrinter1("merged"));




//        try {
//            System.out.format("device id: %s\n", ProtoUtils.getJsonFromMessage(
//                ProtoUtils.decodeMessageBase64(
//                    Common.DeviceId.parser(), "CAESJDEzQjUzNjY3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==")));
//        } catch (InvalidProtocolBufferException e) {
//            e.printStackTrace();
//        }
//        device id: {
//            "os": "ANDROID",
//            "uuid": "13B53667-XXXX-MXXX-NXXX-XXXXXXXXXXXX"
//        }





        tp.run();


    }



}
