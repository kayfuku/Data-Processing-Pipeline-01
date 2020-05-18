package edu.usfca.dataflow.transforms;

import static edu.usfca.dataflow.transforms.__Utils.getCanonicalDeviceId;
import static edu.usfca.dataflow.transforms.__Utils.getMultiSet;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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

public class __TestsWith04LargeDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 50 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  // @Rule
  // public Timeout timeout = Timeout.millis(50000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  final static String PATH_TO_FILE = "../judge/resources/sample-large.txt";

  @Test
  public void test() {
    // this is a dummy test. freebies!
  }

  @Test
  public void test04large_01common() {
    PCollection<PurchaserProfile> actualPp =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents());

    {

      PAssert.that(actualPp).satisfies(out -> {

        assertEquals(10000, Iterables.size(out));
        assertEquals(10000,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 1).count());

        return null;
      });
    }

    PCollection<PurchaserProfile> actualMerged = actualPp.apply(new MergeProfiles());

    {
      PAssert.that(actualMerged).satisfies(out -> {

        assertEquals(2401, Iterables.size(out));

        assertEquals(567,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 2).count());
        assertEquals(556,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 3).count());
        assertEquals(524,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() > 4).count());

        return null;
      });
    }
    tp.run();
  }

  @Test
  public void __shareable__04large_02IAPP() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected =
        Arrays
            .asList("CgbsuZjtgqgQmgMYodmzAQ==", "Cg1hcHAu67mE67mU66m0EK0FGOnLjAM=", "CghpZDY4NjQ4NhDxBRi0pcoD",
                "CgbliqDmsrkQiwMYt6K1AQ==", "Cg1hcHAu7Kec7J6l66m0ENQFGKKy3QM=", "Cgbsu6TtlLwQuwMY4ub7AQ==",
                "CghpZDQ4NjY4NhDDBRj7iZAD", "CgVpZDQ4NhDrBRiTwZgD", "CgphcHAu64OJ66m0EOMFGJzowAM=",
                "CgphcHAu652866m0EOsFGKiq4QM=", "CgbtlLzsnpAQsAMYvKDVAQ==", "CgVpZDY4NhDVBRjf2tsD")
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

      assertEquals(12,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getTotalAmount() > 1000000).count());
      assertEquals(10,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getTotalAmount() > 3000000).count());
      assertEquals(8,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getTotalAmount() > 5000000).count());

      assertEquals(expected, actual);
      return null;
    });

    tp.run();
  }

  @Test
  public void __shareable__04large_02HighSpender() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDc2RDUxOTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDg1RkNFNEM2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDRFMTlFQjUxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDREMzA3OTYzLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDc0OUNERkE0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEY1OTdFQjk4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEM3QkM3QTQ3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDUxQzYwRTA5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(3, 10000L, new ImmutableSet.Builder<String>().add("id686486").build())))
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
      Multiset<String> expectedIds = getMultiSet("CAESJDg0MDM4NzM1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhBMEVFQUIxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDBGOEZDNDM1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDlFNTMwODUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFGNUZDMUM0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged.apply(
              new ExtractHighSpenders(4, 10000L, new ImmutableSet.Builder<String>().add("id486", "id686").build())))

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

  @Test
  public void __shareable__04large_02Addicts() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDU0MkU2N0I4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk5MjNDRDhELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJERBRDlBRTg2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDM2OUVCNjNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDUwNzdFNENBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged.apply(new ExtractAddicts("id486", 3))).satisfies(out -> {

        ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJERBMUU5RkE1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged.apply(new ExtractAddicts("id686", 4))).satisfies(out -> {
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

  // --

  @Test
  public void __shareable_all() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected =
        Arrays
            .asList("CgbsuZjtgqgQmgMYodmzAQ==", "Cg1hcHAu67mE67mU66m0EK0FGOnLjAM=", "CghpZDY4NjQ4NhDxBRi0pcoD",
                "CgbliqDmsrkQiwMYt6K1AQ==", "Cg1hcHAu7Kec7J6l66m0ENQFGKKy3QM=", "Cgbsu6TtlLwQuwMY4ub7AQ==",
                "CghpZDQ4NjY4NhDDBRj7iZAD", "CgVpZDQ4NhDrBRiTwZgD", "CgphcHAu64OJ66m0EOMFGJzowAM=",
                "CgphcHAu652866m0EOsFGKiq4QM=", "CgbtlLzsnpAQsAMYvKDVAQ==", "CgVpZDY4NhDVBRjf2tsD")
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

      assertEquals(12,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getTotalAmount() > 1000000).count());
      assertEquals(10,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getTotalAmount() > 3000000).count());
      assertEquals(8,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getTotalAmount() > 5000000).count());

      assertEquals(expected, actual);
      return null;
    });

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDREMzA3OTYzLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDc2RDUxOTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEM3QkM3QTQ3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDRFMTlFQjUxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDg1RkNFNEM2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(3, 20000L, new ImmutableSet.Builder<String>().add("id686486").build())))
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
      Multiset<String> expectedIds = getMultiSet("CAESJDBGOEZDNDM1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFGNUZDMUM0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDg0MDM4NzM1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDlFNTMwODUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhBMEVFQUIxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged.apply(
              new ExtractHighSpenders(4, 10000L, new ImmutableSet.Builder<String>().add("id686", "id486").build())))

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
      Multiset<String> expectedIds = getMultiSet("CAESJDU0MkU2N0I4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk5MjNDRDhELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJERBRDlBRTg2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDM2OUVCNjNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDUwNzdFNENBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged.apply(new ExtractAddicts("id486", 3))).satisfies(out -> {

        ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJERBMUU5RkE1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged.apply(new ExtractAddicts("id686", 4))).satisfies(out -> {
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
}
