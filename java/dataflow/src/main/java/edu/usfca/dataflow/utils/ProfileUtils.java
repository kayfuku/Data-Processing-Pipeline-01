package edu.usfca.dataflow.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.transforms.PurchaserProfiles;
import edu.usfca.dataflow.transforms.PurchaserProfiles.Accumulator;
import edu.usfca.dataflow.transforms.PurchaserProfiles.Accumulator.PurchaseAcc;
import edu.usfca.protobuf.Profile;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile.Purchase;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class ProfileUtils {

  // Merge accumulators into merged.
  public static Accumulator mergePurchaserProfiles(Iterable<Accumulator> accumulators) {
    Accumulator merged = null;

    for (Accumulator acc : accumulators) {

      if (merged == null) {
        // Hold the first one.
        merged = acc;
        continue;
      }

      // Merge device id.
      if (!StringUtils.isBlank(acc.getId().getUuid())) {
        merged.setId(acc.getId());
      }

      // Merge purchase total.
      merged.setPurchaseTotal(merged.getPurchaseTotal() + acc.getPurchaseTotal());

      // bundle, amount, num purchases and event days
      Map<String, PurchaseAcc> bundleToPurchaseMerged = merged.getBundleToPurchase();
      Map<String, PurchaseAcc> bundleToPurchaseAcc = acc.getBundleToPurchase();

      for (Map.Entry<String, PurchaseAcc> entry : bundleToPurchaseAcc.entrySet()) {
        // For each bundle
        String bundleAcc = entry.getKey();
        PurchaseAcc purchaseAcc = entry.getValue();

        if (!bundleToPurchaseMerged.containsKey(bundleAcc)) {
          bundleToPurchaseMerged.put(bundleAcc, purchaseAcc);
        } else {
          // Get the purchaseAcc for the bundle and update.
          PurchaseAcc purchaseMerged = bundleToPurchaseMerged.get(bundleAcc);
          purchaseMerged.addTotalAmount(purchaseAcc.getTotalAmount());
          purchaseMerged.addNumPurchases(purchaseAcc.getNumPurchases());
          purchaseMerged.addAllEventDays(new ArrayList<Integer>(purchaseAcc.getEventDays()));
        }
      }
    }

    return merged;
  }

  // Merge pp into acc.
  public static Accumulator mergeTwoPurchaserProfiles(Accumulator acc, PurchaserProfile pp) {

    // Merge device id.
    acc.setId(pp.getId());

    // Merge purchase total.
    acc.setPurchaseTotal(acc.getPurchaseTotal() + pp.getPurchaseTotal());

    // Merge bundle, amount, num purchases and event days.
    String bundlePp = pp.getBundles(0);
    Purchase purchasePp = pp.getPurchases(0);
    Map<String, PurchaseAcc> bundleToPurchaseAcc = acc.getBundleToPurchase();
    if (!bundleToPurchaseAcc.containsKey(bundlePp)) {
      PurchaseAcc purchaseAcc = new PurchaseAcc(
          purchasePp.getTotalAmount(), purchasePp.getNumPurchases(), purchasePp.getEventDaysList());
      bundleToPurchaseAcc.put(bundlePp, purchaseAcc);
    } else {
      // Get the purchaseAcc for the bundle and update.
      PurchaseAcc purchaseAcc = bundleToPurchaseAcc.get(bundlePp);
      purchaseAcc.addTotalAmount(purchasePp.getTotalAmount());
      purchaseAcc.addNumPurchases(purchasePp.getNumPurchases());
      purchaseAcc.addAllEventDays(purchasePp.getEventDaysList());
    }

    return acc;
  }







}





































