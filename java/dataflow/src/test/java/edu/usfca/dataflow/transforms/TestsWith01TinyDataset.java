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
import edu.usfca.dataflow.utils.ProtoUtils;
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

public class TestsWith01TinyDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 10 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  // @Rule
  // public Timeout timeout = Timeout.millis(10000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  final static String PATH_TO_FILE = "../judge/resources/sample-tiny.txt";

  @Test
  public void test() {
    // this is a dummy test. freebies!
  }

  // sample-tiny.txt contains 10 logs with distinct device IDs.
  @Test
  public void __shareable__01tiny_01common() {
    PCollection<PurchaserProfile> actualPp =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJEYxOTlERkRFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualPp).satisfies(out -> {

        assertEquals(15, Iterables.size(out));

        ImmutableMultiset<String> actualIds =
            new ImmutableMultiset.Builder<String>().addAll(StreamSupport.stream(out.spliterator(), false)
                .map(pp -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(pp.getId())))
                .collect(Collectors.toList())).build();

        assertEquals(15,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 1).count());

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    PCollection<PurchaserProfile> actualMerged = actualPp.apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAISJEYxOTlERkRFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged).satisfies(out -> {

        assertEquals(7, Iterables.size(out));

        ImmutableMultiset<String> actualIds =
            new ImmutableMultiset.Builder<String>().addAll(StreamSupport.stream(out.spliterator(), false)
                .map(pp -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(pp.getId())))
                .collect(Collectors.toList())).build();

        assertEquals(2,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 1).count());

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }
    tp.run();
  }

  @Test
  public void __shareable__01tiny_02IAPP() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected =
        Arrays
            .asList("Cg1hcHAu7Kec7J6l66m0EAEY+r8B", "CgphcHAu652866m0EAMYzRk=", "CgVpZDQ4NhABGP1f",
                "CghpZDQ4NjY4NhABGM4O", "Cg1hcHAu67mE67mU66m0EAEYiHE=", "CghpZDY4NjQ4NhADGI1k", "CgbliqDmsrkQARjSCA==")
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
  public void __shareable__01tiny_02HighSpender() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(1, 1L, new ImmutableSet.Builder<String>().add("id686486").build())))
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
      Multiset<String> expectedIds = getMultiSet("CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(1, 300L, new ImmutableSet.Builder<String>().add("id686486").build())))
          .satisfies(out -> {

            ImmutableMultiset actualIds = new ImmutableMultiset.Builder<String>()
                .addAll(StreamSupport.stream(out.spliterator(), false)
                    .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                .build();

            assertEquals(expectedIds, actualIds);
            return null;
          });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(1, 1000L, new ImmutableSet.Builder<String>().add("id686486").build())))
          .satisfies(out -> {

            ImmutableMultiset actualIds = new ImmutableMultiset.Builder<String>()
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

  @Test
  public void __shareable__01tiny_02Addicts() {
    PCollection<PurchaserProfile> actualMerged = tp
        .apply(TextIO.read().from(PATH_TO_FILE))
//        .apply(TextIO.read().from(PATH_TO_FILE2))
        .apply(new GetProfilesFromEvents())
        .apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert.that(actualMerged.apply(new ExtractAddicts("id686486", 1))).satisfies(out -> {

        ImmutableMultiset actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    {
      PAssert.that(actualMerged.apply(new ExtractAddicts("id686486", 2))).empty();
    }

    tp.run();
  }



}
