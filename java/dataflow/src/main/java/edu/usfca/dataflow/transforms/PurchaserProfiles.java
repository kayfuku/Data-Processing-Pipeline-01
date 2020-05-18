package edu.usfca.dataflow.transforms;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.utils.CommonUtils;
import edu.usfca.dataflow.utils.ProfileUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Event.PurchaseEvent;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import edu.usfca.dataflow.utils.LogParser;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile.Purchase;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.Serializable;
import java.util.*;

/**
 * Task B.
 */
public class PurchaserProfiles {

  /**
   * Input PCollection contains log messages to be parsed using {@link LogParser#getIdAndPurchaseEvent(String)}.
   *
   * These log messages are assumed to be for an n-day period (where the exact value of n is not important to us yet).
   * Just think of it as a weekly or monthly dataset.
   *
   * Output PCollection must contain "PurchaseProfile" protos, one for each PurchaseEvent.
   *
   * Aggregation of these profiles happens in a different PTransform (mainly for unit test purposes)
   * {@link MergeProfiles}.
   *
   * If {@link LogParser#getIdAndPurchaseEvent(String)} returns a null value (because the log message is
   * invalid/corrupted), then simply ignore the returned value (don't include it in your output PC).
   *
   * As such, the output PCollection will always have no more elements than the input PCollection.
   */
    public static class GetProfilesFromEvents
        extends PTransform<PCollection<String>, PCollection<PurchaserProfile>> {

        @Override
        public PCollection<PurchaserProfile> expand(PCollection<String> input) {

            PCollection<PurchaserProfile> ret = input // PC<String>
                .apply(ParDo.of(new DoFn<String, PurchaserProfile>() {
                    @ProcessElement
                    public void anyName(@Element String jsonLogAsLine, OutputReceiver<PurchaserProfile> out) {

                        // Extract info from logs.
                        KV<DeviceId, PurchaseEvent> devIdToPurchaseEvent = LogParser.getIdAndPurchaseEvent(jsonLogAsLine);
                        DeviceId deviceId = devIdToPurchaseEvent.getKey();
                        String bundle = devIdToPurchaseEvent.getValue().getAppBundle();
                        int amount = devIdToPurchaseEvent.getValue().getAmount();
                        long eventAt = devIdToPurchaseEvent.getValue().getEventAt();

                        // Set deviceId and purchaseTotal to PurchaserProfile.
                        PurchaserProfile.Builder pp = PurchaserProfile.newBuilder();
                        pp.setId(deviceId)
                            .setPurchaseTotal(1);

                        // Set bundle, amount, numPurchases and eventAt.
                        Purchase.Builder purchase = Purchase.newBuilder();
                        purchase.setTotalAmount(amount)
                            .setNumPurchases(1)
                            .addEventDays(CommonUtils.millisToDay(eventAt) - 18310);
                        pp.addBundles(bundle).addPurchases(purchase.build());

                        out.output(pp.build());
                    }
                })); // PC<PurchaserProfile>

            return ret;
        }
    }


  /**
   * This is expected to be called on the output of {@link GetProfilesFromEvents} (unit tests do that, for instance).
   * Given (yet-to-be-merged) PC of PurchaserProfile protos, this PTransform must combine profiles (by DeviceId).
   */
    public static class MergeProfiles
        extends PTransform<PCollection<PurchaserProfile>, PCollection<PurchaserProfile>> {

        @Override
        public PCollection<PurchaserProfile> expand(PCollection<PurchaserProfile> input) {

            PCollection<PurchaserProfile> merged = input // PC<PurchaserProfile>
                .apply(MapElements.into(TypeDescriptors.kvs(
                    TypeDescriptor.of(DeviceId.class), TypeDescriptor.of(PurchaserProfile.class)))
                    .via((PurchaserProfile pp) -> KV.of(pp.getId(), pp))) // PC<KV<DeviceId, PurchaserProfile>>
                // Merge device profile per device id.
                .apply(Combine.perKey(new Combine.CombineFn<PurchaserProfile, Accumulator, PurchaserProfile>() {
                    // Initialize an accumulator.
                    @Override public Accumulator createAccumulator() {
                        return new Accumulator(DeviceId.getDefaultInstance(), 0);
                    }
                    // Define how to accumulate the values.
                    @Override public Accumulator addInput(Accumulator acc, PurchaserProfile input) {

                        Accumulator pp = ProfileUtils.mergeTwoPurchaserProfiles(acc, input);

                        return pp;
                    }
                    // Accumulate partial results.
                    @Override public Accumulator mergeAccumulators(Iterable<Accumulator> accs) {

                        Accumulator acc = ProfileUtils.mergePurchaserProfiles(accs);

                        return acc;
                    }
                    // Convert Accumulator type to PurchaserProfile type.
                    @Override public PurchaserProfile extractOutput(Accumulator acc) {
                        PurchaserProfile.Builder pp = PurchaserProfile.newBuilder();
                        // id
                        pp.setId(acc.getId());
                        // purchase total
                        pp.setPurchaseTotal(acc.getPurchaseTotal());
                        // bundle, total amount, num purchases, event days
                        for (Map.Entry<String, Accumulator.PurchaseAcc> entry : acc.getBundleToPurchase().entrySet()) {
                            String bundle = entry.getKey();
                            Accumulator.PurchaseAcc purchaseAcc = entry.getValue();
                            // purchase
                            Purchase.Builder purchase = Purchase.newBuilder();
                            purchase.setTotalAmount(purchaseAcc.getTotalAmount())
                                .setNumPurchases(purchaseAcc.getNumPurchases())
                                .addAllEventDays(purchaseAcc.getEventDays());
                            // Add bundle and purchase to MapEntry.
                            pp.addBundles(bundle).addPurchases(purchase.build());
                        }

                        return pp.build();
                    }
                })) // PC<KV<DeviceId, PurchaserProfile>>
                .apply(Values.create()); // PC<PurchaserProfile>

            return merged;
        }
    }


    public static class Accumulator implements Serializable {
        private DeviceId id;
        private int purchaseTotal;
        private Map<String, PurchaseAcc> bundleToPurchase;

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Accumulator that = (Accumulator) o;
            return purchaseTotal == that.purchaseTotal && Objects.equals(id, that.id) && Objects
                .equals(bundleToPurchase, that.bundleToPurchase);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, purchaseTotal, bundleToPurchase);
        }

        public String toString() {
            String ret = "";
            try {
                ret = String.format("id: %s\n", ProtoUtils.getJsonFromMessage(id));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

            ret += String.format("purchaseTotal: %d\n", purchaseTotal);
            ret += String.format("bundleToPurchase: %s\n", bundleToPurchase.toString());

            return ret;
        }

        public Accumulator(DeviceId id, int purchaseTotal) {
            this.id = id;
            this.purchaseTotal = purchaseTotal;
            bundleToPurchase = new HashMap<>();
        }

        public int getPurchaseTotal() {
            return purchaseTotal;
        }

        public void setPurchaseTotal(int purchaseTotal) {
            this.purchaseTotal = purchaseTotal;
        }

        public Map<String, PurchaseAcc> getBundleToPurchase() {
            return bundleToPurchase;
        }

        public void setBundleToPurchase(Map<String, PurchaseAcc> bundleToPurchase) {
            this.bundleToPurchase = bundleToPurchase;
        }

        public DeviceId getId() {
            return id;
        }

        public void setId(DeviceId id) {
            this.id = id;
        }

        // Inner Class PurchaseAcc
        public static class PurchaseAcc implements Serializable {
            private long totalAmount;
            private int numPurchases;
            private Set<Integer> eventDays = new HashSet<>();

            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                PurchaseAcc that = (PurchaseAcc) o;
                return totalAmount == that.totalAmount && numPurchases == that.numPurchases && Objects
                    .equals(eventDays, that.eventDays);
            }

            @Override public int hashCode() {
                return Objects.hash(totalAmount, numPurchases, eventDays);
            }

            public PurchaseAcc(long totalAmount, int numPurchases, List<Integer> eventDays) {
                this.totalAmount = totalAmount;
                this.numPurchases = numPurchases;
                this.eventDays.addAll(eventDays);
            }

            public int getNumPurchases() {
                return numPurchases;
            }

            public void setNumPurchases(int numPurchases) {
                this.numPurchases = numPurchases;
            }

            public long getTotalAmount() {
                return totalAmount;
            }

            public void setTotalAmount(long totalAmount) {
                this.totalAmount = totalAmount;
            }

            public Set<Integer> getEventDays() {
                return this.eventDays;
            }

            public void setEventDays(Set<Integer> eventDays) {
                this.eventDays = eventDays;
            }

            public void addTotalAmount(long amountIn) {
                this.totalAmount += amountIn;
            }

            public void addNumPurchases(int numPurchases) {
                this.numPurchases += numPurchases;
            }

            public void addAllEventDays(List<Integer> eventDaysIn) {
                this.eventDays.addAll(eventDaysIn);
            }

        }

    }





}

















