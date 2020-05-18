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

public class __TestsWith02SmallDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 20 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  // @Rule
  // public Timeout timeout = Timeout.millis(20000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  final static String PATH_TO_FILE = "../judge/resources/sample-small.txt";

  @Test
  public void test() {
    // this is a dummy test. freebies!
  }

  @Test
  public void __shareable__02small_01common() {
    PCollection<PurchaserProfile> actualPp =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJEYxOTlERkRFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJEYxOTlERkRFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU0RTk0QTc3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU0RTk0QTc3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU0RTk0QTc3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZCMURFQUFFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZCMURFQUFFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZCMURFQUFFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENGMUQ1RjIyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENGMUQ1RjIyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENGMUQ1RjIyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJFRjcxRTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJFRjcxRTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJFRjcxRTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJFRjcxRTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ2OEVGQkJELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ2OEVGQkJELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ2OEVGQkJELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNCNDUxNUE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEFFNzlFMkZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEFFNzlFMkZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEM2MkVFMUQwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEM2MkVFMUQwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEM2MkVFMUQwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEI5REIyOEIzLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDgxOTJBN0FDLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDgxOTJBN0FDLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDcxQTQ2NUI4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDcxQTQ2NUI4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDcxQTQ2NUI4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk0OTVCNTYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk0OTVCNTYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk0OTVCNTYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZEQ0JFNjQyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZEQ0JFNjQyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEQ2NTc2NkNCLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDdEQ0E2NDc2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDdEQ0E2NDc2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDlCMUI3NEI0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDlCMUI3NEI0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDlCMUI3NEI0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDlCMUI3NEI0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNBQjEzMjVBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNBQjEzMjVBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNBQjEzMjVBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU5MTk4RDA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU5MTk4RDA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQxMzg5QjA0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQxMzg5QjA0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQxMzg5QjA0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA1NTBCNkE2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA1NTBCNkE2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA1NTBCNkE2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhGMTVBMDZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhGMTVBMDZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhGMTVBMDZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhGMTVBMDZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQzMjI0MEZBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQzMjI0MEZBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQzMjI0MEZBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDQ2MzU0MkFBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDQ2MzU0MkFBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDQ2MzU0MkFBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEMwRTczMUU1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEMwRTczMUU1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFBNDA4QkUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFBNDA4QkUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFBNDA4QkUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEFERkYxM0ZGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDZGM0I1RjY4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEEwOEE3QzNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ4MEEzNDZBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEIyQTg1RTM2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEZEODNCRjlFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJERBRDlBRTg2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualPp).satisfies(out -> {

        assertEquals(100, Iterables.size(out));

        ImmutableMultiset<String> actualIds =
            new ImmutableMultiset.Builder<String>().addAll(StreamSupport.stream(out.spliterator(), false)
                .map(pp -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(pp.getId())))
                .collect(Collectors.toList())).build();

        assertEquals(100,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 1).count());

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    PCollection<PurchaserProfile> actualMerged = actualPp.apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAISJDZGM0I1RjY4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDcxQTQ2NUI4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFBNDA4QkUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENGMUQ1RjIyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhGMTVBMDZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEEwOEE3QzNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEJFRjcxRTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEM2MkVFMUQwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNBQjEzMjVBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDlCMUI3NEI0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEMwRTczMUU1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU0RTk0QTc3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ2OEVGQkJELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDVFRUY0M0QxLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ4MEEzNDZBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEFFNzlFMkZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZEQ0JFNjQyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQzMjI0MEZBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEIyQTg1RTM2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQwN0NDOUM5LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEI5REIyOEIzLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDdEQ0E2NDc2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU5MTk4RDA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDgxOTJBN0FDLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJEYxOTlERkRFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk0OTVCNTYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQxMzg5QjA0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNCNDUxNUE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEZEODNCRjlFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA1NTBCNkE2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEQ2NTc2NkNCLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA2Nzk4RDcyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDQ2MzU0MkFBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJERBRDlBRTg2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEFERkYxM0ZGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZCMURFQUFFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

      PAssert.that(actualMerged).satisfies(out -> {

        assertEquals(41, Iterables.size(out));

        ImmutableMultiset<String> actualIds =
            new ImmutableMultiset.Builder<String>().addAll(StreamSupport.stream(out.spliterator(), false)
                .map(pp -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(pp.getId())))
                .collect(Collectors.toList())).build();

        assertEquals(8,
            StreamSupport.stream(out.spliterator(), false).filter(pp -> pp.getPurchaseTotal() == 2).count());

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }
    tp.run();
  }

  @Test
  public void __shareable__02small_02IAPP() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected = Arrays.asList("CghpZDY4NjQ4NhAJGMLGBg==", "Cg1hcHAu7Kec7J6l66m0EAwYkPQK",
        "Cg1hcHAu67mE67mU66m0EAQYjNoB", "CgphcHAu64OJ66m0EAwY/qYK", "Cgbsu6TtlLwQBxi3/go=", "CgbtlLzsnpAQBBjYxwI=",
        "CghpZDQ4NjY4NhAKGN/2CA==", "CgphcHAu652866m0EAoY+fgB", "CgbsuZjtgqgQCBjFgAM=", "CgVpZDY4NhAFGP6DAQ==",
        "CgbliqDmsrkQAxjhEA==", "CgVpZDQ4NhAFGPZx").stream().map(b64 -> {
          try {
            return ProtoUtils.decodeMessageBase64(InAppPurchaseProfile.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        }).collect(Collectors.toSet());

    PAssert.that(actualMerged.apply(new ExtractInAppPurchaseData())).satisfies(out -> {
      Set<InAppPurchaseProfile> actual = StreamSupport.stream(out.spliterator(), false).collect(Collectors.toSet());

      assertEquals(1,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getBundle().equals("id686")).count());
      assertEquals(1,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getBundle().equals("id486")).count());

      assertEquals(expected, actual);
      return null;
    });

    tp.run();
  }

  @Test
  public void __shareable__02small_02HighSpender() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ2OEVGQkJELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDhGMTVBMDZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged.apply(
              new ExtractHighSpenders(1, 1000L, new ImmutableSet.Builder<String>().add("id686", "id486").build())))
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
  public void __shareable__02small_02Addicts() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDA1NTBCNkE2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU0RTk0QTc3LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEMwRTczMUU1LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDlCMUI3NEI0LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDFBNDA4QkUwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDU5MTk4RDA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert.that(actualMerged.apply(new ExtractAddicts("커피", 1))).satisfies(out -> {

        ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    {
      PAssert.that(actualMerged.apply(new ExtractAddicts("커피", 2))).empty();
    }

    tp.run();
  }

  // --------

  // This is one of the hidden tests in Project 3.
  @Test
  public void __shareable_all() {
    PCollection<PurchaserProfile> actualMerged =
        tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    Set<InAppPurchaseProfile> expected = Arrays.asList("CghpZDY4NjQ4NhAJGMLGBg==", "Cg1hcHAu7Kec7J6l66m0EAwYkPQK",
        "Cg1hcHAu67mE67mU66m0EAQYjNoB", "CgphcHAu64OJ66m0EAwY/qYK", "Cgbsu6TtlLwQBxi3/go=", "CgbtlLzsnpAQBBjYxwI=",
        "CghpZDQ4NjY4NhAKGN/2CA==", "CgphcHAu652866m0EAoY+fgB", "CgbsuZjtgqgQCBjFgAM=", "CgVpZDY4NhAFGP6DAQ==",
        "CgbliqDmsrkQAxjhEA==", "CgVpZDQ4NhAFGPZx").stream().map(b64 -> {
          try {
            return ProtoUtils.decodeMessageBase64(InAppPurchaseProfile.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        }).collect(Collectors.toSet());

    PAssert.that(actualMerged.apply(new ExtractInAppPurchaseData())).satisfies(out -> {
      Set<InAppPurchaseProfile> actual = StreamSupport.stream(out.spliterator(), false).collect(Collectors.toSet());

      assertEquals(1,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getBundle().equals("id686")).count());
      assertEquals(1,
          StreamSupport.stream(out.spliterator(), false).filter(iapp -> iapp.getBundle().equals("id486")).count());

      assertEquals(expected, actual);
      return null;
    });


    {
      Multiset<String> expectedIds = getMultiSet("CAESJEU2RUJBQjA4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDQ2OEVGQkJELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDQ2MzU0MkFBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDNCNDUxNUE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
          .that(actualMerged
              .apply(new ExtractHighSpenders(1, 1000L, new ImmutableSet.Builder<String>().add("id686486").build())))
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
      Multiset<String> expectedIds = getMultiSet("CAESJEJFRjcxRTNGLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDgxOTJBN0FDLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDdEQ0E2NDc2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJERBRDlBRTg2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDZEQ0JFNjQyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJDQ2MzU0MkFBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJEFFNzlFMkZFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDA1NTBCNkE2LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAISJEYxOTlERkRFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
          "CAESJDk0OTVCNTYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert.that(actualMerged.apply(new ExtractAddicts("id486686", 1))).satisfies(out -> {

        ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
            .addAll(StreamSupport.stream(out.spliterator(), false)
                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
            .build();

        assertEquals(expectedIds, actualIds);
        return null;
      });
    }

    {
      PAssert.that(actualMerged.apply(new ExtractAddicts("id486686", 2))).empty();
    }

    tp.run();
  }
}
