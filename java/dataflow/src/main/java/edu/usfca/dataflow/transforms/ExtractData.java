package edu.usfca.dataflow.transforms;

import java.util.*;

import edu.usfca.dataflow.utils.CommonUtils;
import edu.usfca.protobuf.Common;
import org.apache.avro.generic.GenericData;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.StringUtils;

import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile.Purchase;


/**
 *
 */
public class ExtractData {
  /**
   * This will be applied to the output of {@link MergeProfiles}.
   */
  public static class ExtractInAppPurchaseData
      extends PTransform<PCollection<PurchaserProfile>, PCollection<InAppPurchaseProfile>> {

    @Override
    public PCollection<InAppPurchaseProfile> expand(PCollection<PurchaserProfile> input) {

      TupleTag<String> bundleTag = new TupleTag<String>() {};
      TupleTag<KV<String, Long>> amountTag = new TupleTag<KV<String, Long>>() {};

      PCollectionTuple tuple = input // PC<PurchaserProfile>
          .apply(ParDo.of(new DoFn<PurchaserProfile, String>() {
            @ProcessElement
            public void parseLine(@Element PurchaserProfile pp, MultiOutputReceiver out) {
              // Get distinct bundles per PurchaserProfile that was aggregated by device id.
              // And get the amount for each bundle.
              List<String> bundles = pp.getBundlesList();
              List<Purchase> purchases = pp.getPurchasesList();
              for (int i = 0; i < bundles.size(); i++) {
                String bundle = bundles.get(i);
                // Emit distinct bundles for each device id (purchaser).
                out.get(bundleTag).output(bundle);
                // Emit amount for each bundle.
                out.get(amountTag).output(KV.of(bundle, purchases.get(i).getTotalAmount()));
              }
            }
          }).withOutputTags(bundleTag, TupleTagList.of(amountTag)));

      // Get PC<KV<String (bundle), Long (num of purchasers)>>.
      PCollection<KV<String, Long>> numPurchasersPerBundle = tuple.get(bundleTag) // PC<String (bundle)>
          .apply(Count.perElement());

      // Get PC<KV<String (bundle), Long (total amount)>>.
      PCollection<KV<String, Long>> totalAmountPerBundle = tuple.get(amountTag) // PC<KV<String (bundle), Long (amount)>>
          .apply(Combine.perKey(new SerializableFunction<Iterable<Long>, Long>() {
            @Override public Long apply(Iterable<Long> input) {
              long totalAmount = 0;
              for (Long amount : input) {
                totalAmount += amount;
              }
              return totalAmount;
            }
          })); // PC<KV<String (bundle), Long (total amount)>>

      // Create InAppPurchaseProfile using CoGroupByKey.
      TupleTag<Long> numPurchasersTag = new TupleTag<Long>() {};
      TupleTag<Long> totalAmountTag = new TupleTag<Long>() {};
      PCollection<KV<String, CoGbkResult>> combinedPc =
          KeyedPCollectionTuple.of(numPurchasersTag, numPurchasersPerBundle).and(totalAmountTag, totalAmountPerBundle)
              .apply(CoGroupByKey.create());

      PCollection<InAppPurchaseProfile> ret = combinedPc // PC<KV<String (bundle), CoGbkResult>>
          .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, InAppPurchaseProfile>() {
            @ProcessElement
            public void process(@Element KV<String, CoGbkResult> elem, OutputReceiver<InAppPurchaseProfile> out) {
              final String bundle = elem.getKey();
              Long numPurchasers = elem.getValue().getOnly(numPurchasersTag);
              Long totalAmount = elem.getValue().getOnly(totalAmountTag);

              InAppPurchaseProfile.Builder iapp = InAppPurchaseProfile.newBuilder();
              iapp.setBundle(bundle)
                  .setNumPurchasers(numPurchasers)
                  .setTotalAmount(totalAmount);

              out.output(iapp.build());
            }
          })); // PC<InAppPurchaseProfile>

      return ret;
    }
  }

  /**
   * This will be applied to the output of {@link MergeProfiles}.
   *
   * This PTransform should return a PC of DeviceIds that are "high spenders".
   *
   * It takes three parameters (to define high spenders):
   *
   * (1) an integer, "x",
   *
   * (2) an integer, "y", and
   *
   * (3) a set of "bundles".
   *
   * Return a PC of DeviceIds who
   *
   * (i) made {@code x} or more purchases AND
   *
   * (ii) spent {@code y} or more
   *
   * across the apps specified in the set {@code bundles}.
   *
   */
  public static class ExtractHighSpenders extends PTransform<PCollection<PurchaserProfile>, PCollection<DeviceId>> {

    final int numPurchases;
    final long totalAmount;
    final Set<String> bundles;

    public ExtractHighSpenders(int numPurchases, long totalAmount, Set<String> bundles) {
      this.numPurchases = numPurchases;
      this.totalAmount = totalAmount;
      this.bundles = bundles;
    }

    @Override
    public PCollection<DeviceId> expand(PCollection<PurchaserProfile> input) {

      PCollection<DeviceId> highSpenders = input // PC<PurchaserProfile>
          .apply(ParDo.of(new DoFn<PurchaserProfile, DeviceId>() {
            @ProcessElement
            public void anyName(@Element PurchaserProfile pp, OutputReceiver<DeviceId> out) {
              int numPurchasesX = 0;
              long amountTotalY = 0L;

              List<String> bundlesList = pp.getBundlesList();
              List<Purchase> purchases = pp.getPurchasesList();
              for (int i = 0; i < bundlesList.size(); i++) {
                String bundle = bundlesList.get(i);
                if (bundles.contains(bundle)) {
                  numPurchasesX += purchases.get(i).getNumPurchases();
                  amountTotalY += purchases.get(i).getTotalAmount();
                }
              }

              if (numPurchasesX >= numPurchases && amountTotalY >= totalAmount) {
                // Emit this device id who meets the conditions.
                out.output(pp.getId());
              }
            }
          })); // PC<DeviceId>

      return highSpenders;
    }
  }


  /**
   * This will be applied to the output of {@link MergeProfiles}.
   *
   * This PTransform should return a PC of DeviceIds (users) who are considered as "addicts."
   *
   * It takes two parameters: String parameter "bundle" that specifies the bundle of our interests and integer parameter
   * "x" representing the number of consecutive (calendar) days.
   *
   * Return a PC of DeviceIds of the users who made at least one purchase per day for {@code x} consecutive (calendar)
   * days (in UTC timezone) for the given app ("bundle").
   *
   * For instance, if a user made a purchase on "2020-02-01T10:59:59.999Z", another on "2020-02-02T20:59:59.999Z", and
   * one more on "2020-02-03T23:59:59.999Z" (for the same app), then this user would be considered as an addict when
   * {@code x=3} (we only consider the "dates", and since this user made at least one purchase on each of Feb 01, Feb
   * 02, and Feb 03). On the other hand: if a user made a purchase on "2020-02-01T10:59:59.999Z", another on
   * "2020-02-02T20:59:59.999Z", and * one more on "2020-02-02T23:59:59.999Z" (for the same app), then this user would
   * NOT be considered as an addict. (this user made one purchase on Feb 01 and two on Feb 02, so that is NOT 3
   * consecutive days).
   *
   * You may find https://currentmillis.com/ useful (for checking unix millis and converting it to UTC date).
   *
   * NOTE: If "bundle" is null/blank, throw IllegalArgumentException (use the usual apache.lang3 utility). If "x" is 0
   * or less, then throw IllegalArgumentException as well.
   *
   */
  public static class ExtractAddicts extends PTransform<PCollection<PurchaserProfile>, PCollection<DeviceId>> {

    final String bundleGiven;
    final int CONSEC_DAYS;

    public ExtractAddicts(String bundleGiven, int x) {
      if (StringUtils.isBlank(bundleGiven)) {
        throw new IllegalArgumentException("");
      }
      if (x <= 0) {
        throw new IllegalArgumentException("");
      }
      this.bundleGiven = bundleGiven;
      this.CONSEC_DAYS = x;
    }

    public static int millisToDay(long millisInUtc) {
      return (int) (millisInUtc / 1000L / 3600L / 24L);
    }

    public static boolean checkConsecutiveDays(List<Integer> days, int k) {
      if (days == null || days.size() == 0 || days.size() < k) {
        return false;
      }
      if (days.size() >= 1 && k == 1) {
        return true;
      }

      int minDay = Integer.MAX_VALUE;
      int maxDay = Integer.MIN_VALUE;
      for (int day : days) {
        minDay = Math.min(minDay, day);
        maxDay = Math.max(maxDay, day);
      }
      int[] countArr = new int[maxDay - minDay + 1];
      for (int day : days) {
        countArr[day - minDay]++;
      }

      int count = 0;
      for (int i = 1; i < countArr.length; i++) {
        if (countArr[i] >= 1 && countArr[i - 1] >= 1) {
          count++;
          if (count == k - 1) {
            return true;
          }
        } else {
          count = 0;
        }
      }

      return false;
    }

    @Override
    public PCollection<DeviceId> expand(PCollection<PurchaserProfile> input) {

      PCollection<DeviceId> addicts = input // PC<PurchaserProfile>
          .apply(ParDo.of(new DoFn<PurchaserProfile, DeviceId>() {
            @ProcessElement
            public void anyName(@Element PurchaserProfile pp, OutputReceiver<DeviceId> out) {

              List<String> bundlesList = pp.getBundlesList();
              List<Purchase> purchases = pp.getPurchasesList();
              for (int i = 0; i < bundlesList.size(); i++) {
                String bundle = bundlesList.get(i);
                if (bundle.equals(bundleGiven)) {
                  // Check if there is at least one purchase per day for CONSEC_DAYS days for the given app.
                  if (checkConsecutiveDays(purchases.get(i).getEventDaysList(), CONSEC_DAYS)) {
                    out.output(pp.getId());
                    return;
                  }
                }
              }

            }
          })); // PC<DeviceId>

      return addicts;
    }
  }

}



















