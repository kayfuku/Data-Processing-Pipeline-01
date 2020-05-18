package edu.usfca.dataflow.transforms;

import com.google.common.collect.Iterables;
import edu.usfca.dataflow.transforms.PurchaserProfiles.GetProfilesFromEvents;
import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.dataflow.utils.CommonUtils;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

public class TestsWith05ByteSize {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 15 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  @Rule
  public Timeout timeout = Timeout.millis(15000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  /**
   * This method returns an estimated total bytes of "repeated PurchaseEvent" proto messages, assuming that the parser
   * (from JSON logs to PurchaseEvent) is correctly populating all relevant fields. The goal in this project is for you
   * to figure out the way you define "PurchaserProfile" properly, so that its overall size would be much smaller than
   * repeated PurchaseEvents.
   */
  static long getRepeatedPESize(String file) {
    switch (file) {
      case FILE00:
        return 610;
      case FILE01:
        return 574;
      case FILE02:
        return 3595;
      case FILE03:
        return 176968;
      case FILE04:
        return 354639;
      case FILE05:
        return 26844;
      case FILE06:
        return 177570;
      case FILE07:
        return 8861808;
      default:
        throw new IllegalArgumentException("file is not recognized");
    }
  }

  // FILE01 - FILE04 are the same as project 3.
  final static String FILE00 = "../judge/resources/sample-smaller.txt";
  final static String FILE01 = "../judge/resources/sample-tiny.txt";
  final static String FILE02 = "../judge/resources/sample-small.txt";
  final static String FILE03 = "../judge/resources/sample-medium.txt";
  final static String FILE04 = "../judge/resources/sample-large.txt";

  // FILE05 - FILE07 are larger than the sample-* files.
  final static String FILE05 = "../judge/resources/test-tiny.txt";
  final static String FILE06 = "../judge/resources/test-small.txt";
  final static String FILE07 = "../judge/resources/test-medium*.txt";

  void __testHelper(String file, double ratio) {
    __testHelper(file, ratio, false);
  }

  void __testHelper(String file, double ratio, boolean applyMerge) {
    PCollection<String> inputLogs = tp.apply(TextIO.read().from(file));

    PCollection<PurchaserProfile> actualPp = inputLogs.apply(new GetProfilesFromEvents());
    if (applyMerge) {
      actualPp = actualPp.apply(new MergeProfiles());
    }

    // Compute the total size of "actual purchaser profile" (excluding DeviceId size).
    PCollection<Long> totalPPSize = actualPp // PC<PurchaserProfile>
        .apply(MapElements.into(TypeDescriptors.longs())
          .via((SerializableFunction<PurchaserProfile, Long>) pp ->
              (long) (pp.getSerializedSize() - pp.getId().getSerializedSize()))) // PC<Long>
        .apply(Combine.globally(Sum.ofLongs())); // PC<Long>

    PAssert.that(totalPPSize).satisfies(out -> {
      assertEquals(1, Iterables.size(out));
      final long totalPP = out.iterator().next();
      final long totalPE = getRepeatedPESize(file);

      System.out.format(
          "[%s] Total Byte Size (Repeated Purchase Event vs Your Proto): %d vs %d (%.2f %%) -- Using %s\n",
          applyMerge ? "PostMerge" : "PreMerge", totalPE, totalPP, ((double) 100. * totalPP / totalPE), file);

      assertTrue(((double) totalPP / totalPE) <= ratio);

      switch ((int) totalPE) {
        case 610:
          assertEquals(FILE00, file);
          break;
        case 574:
          assertEquals(FILE01, file);
          break;
        case 3595:
          assertEquals(FILE02, file);
          break;
        case 176968:
          assertEquals(FILE03, file);
          break;
        case 354639:
          assertEquals(FILE04, file);
          break;
        case 26844:
          assertEquals(FILE05, file);
          break;
        case 177570:
          assertEquals(FILE06, file);
          break;
        case 8861808:
          assertEquals(FILE07, file);
          break;
        default:
          fail();
      }
      return null;
    });

    tp.run();
  }

  @Test
  public void test00SizePreMerge75() {
    __testHelper(FILE00, 0.75);
  }

  @Test
  public void test01SizePreMerge75() {
    __testHelper(FILE01, 0.75);
  }

  @Test
  public void test01SizePreMerge80() {
    __testHelper(FILE01, 0.80);
  }

  @Test
  public void test01SizePreMerge85() {
    __testHelper(FILE01, 0.85);
  }

  @Test
  public void test01SizePreMerge90() {
    __testHelper(FILE01, 0.90);
  }

  // --------------------------------------------
  // New tests added on March 21: "PostMerge" methods check the size of your PCollection after "Merge" operation was
  // performed. Obviously, this PC should be much smaller in size, compared to the size of "repeated PurchaseEvent"s.
  // Reference solution's size is multiplied by 1.05 to accommodate for up to 5% of overhead
  @Test  // Failed. => Passed.
  public void test00SizePostMerge() {
    __testHelper(FILE00, 0.0722 * 1.05, true);
  }
  // Before subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 610 vs 98 (16.07 %) -- Using ../judge/resources/sample-smaller.txt
  // After subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 610 vs 58 (9.51 %) -- Using ../judge/resources/sample-smaller.txt
  // After removing nested: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 610 vs 54 (8.85 %) -- Using ../judge/resources/sample-smaller.txt
  // [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 610 vs 40 (6.56 %) -- Using ../judge/resources/sample-smaller.txt

  @Test
  public void test01SizePostMerge() {
    __testHelper(FILE01, 0.508 * 1.05, true);
  }

  @Test
  public void test02SizePostMerge() {
    __testHelper(FILE02, 0.6076 * 1.05, true);
  }

  @Test
  public void test03SizePostMerge() {
    __testHelper(FILE03, 0.5397 * 1.05, true);
  }

  @Test
  public void test04SizePostMerge() {
    __testHelper(FILE04, 0.5088 * 1.05, true);
  }

  @Test  // Failed. => Passed.
  public void test05SizePostMerge() {
    __testHelper(FILE05, 0.0111 * 1.05, true);
  }
  // Before subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 26844 vs 515 (1.92 %) -- Using ../judge/resources/test-tiny.txt
  // After subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 26844 vs 334 (1.24 %) -- Using ../judge/resources/test-tiny.txt
  // After removing nested: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 26844 vs 320 (1.19 %) -- Using ../judge/resources/test-tiny.txt
  // [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 26844 vs 266 (0.99 %) -- Using ../judge/resources/test-tiny.txt

  @Test  // Failed. => Passed.
  public void test06SizePostMerge() {
    __testHelper(FILE06, 0.0125 * 1.05, true);
  }
  // Before subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 177570 vs 3626 (2.04 %) -- Using ../judge/resources/test-small.txt
  // After subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 177570 vs 2423 (1.36 %) -- Using ../judge/resources/test-small.txt
  // After removing nested: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 177570 vs 2341 (1.32 %) -- Using ../judge/resources/test-small.txt
  // [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 177570 vs 2013 (1.13 %) -- Using ../judge/resources/test-small.txt

  @Test  // Failed. => Passed.
  public void test07SizePostMerge() {
    __testHelper(FILE07, 0.0111 * 1.05, true);
  }
  // Before subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 8861808 vs 168501 (1.90 %) -- Using ../judge/resources/test-medium*.txt
  // After subtracting: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 8861808 vs 108244 (1.22 %) -- Using ../judge/resources/test-medium*.txt
  // After removing nested: [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 8861808 vs 105388 (1.19 %) -- Using ../judge/resources/test-medium*.txt
  // [PostMerge] Total Byte Size (Repeated Purchase Event vs Your Proto): 8861808 vs 87922 (0.99 %) -- Using ../judge/resources/test-medium*.txt

  // --------------------------------------------

  @Test
  public void __shareable__02SizePreMerge75() {
    __testHelper(FILE02, 0.75);
  }

  @Test
  public void __shareable__02SizePreMerge85() {
    __testHelper(FILE02, 0.85);
  }

  @Test
  public void __shareable__02SizePreMerge95() {
    __testHelper(FILE02, 0.95);
  }

  @Test
  public void __shareable__03SizePreMerge75() {
    __testHelper(FILE03, 0.75);
  }

  @Test
  public void __shareable__03SizePreMerge85() {
    __testHelper(FILE03, 0.85);
  }

  @Test
  public void __shareable__03SizePreMerge95() {
    __testHelper(FILE03, 0.95);
  }

  @Test
  public void __shareable__04SizePreMerge75() {
    __testHelper(FILE04, 0.75);
  }

  @Test
  public void __shareable__04SizePreMerge85() {
    __testHelper(FILE04, 0.85);
  }

  @Test
  public void __shareable__04SizePreMerge95() {
    __testHelper(FILE04, 0.95);
  }




  final static String PATH_TO_FILE = "../judge/resources/sample-tiny.txt";
  final static String PATH_TO_FILE2 = "../judge/resources/sample-tiny-for-debug.txt";
  final static String PATH_TO_FILE_SMALL = "../judge/resources/sample-small.txt";

  @Test
  public void myTest00() {

    // dummy commit for lec 30

    PCollection<PurchaserProfile> actualMerged = tp
        .apply(TextIO.read().from(PATH_TO_FILE_SMALL))
        .apply(new GetProfilesFromEvents())
        .apply(new MergeProfiles());

    actualMerged.apply(new CommonUtils.StrPrinter1("test"));

    tp.run();


  }







}
