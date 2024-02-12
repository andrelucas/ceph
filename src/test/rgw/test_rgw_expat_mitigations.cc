/**
 * @file test_rgw_expat_mitigations.cc
 * @author André Lucas (alucas@akamai.com)
 * @brief Check for presence and operation of Expat 'Billion Laughs' attack
 * mitigations.
 * @version 0.1
 * @date 2024-02-12
 *
 * @copyright Copyright (c) 2024
 */

// XML_DTD must be defined or the mitigations won't be declared in <expat.h>.
#define XML_DTD 1

#include <gtest/gtest.h>
#include <expat.h>

TEST(RGWExpatMitigations, Linkage) {
    // Make sure we can link, not just compile, with the XML mitigations. This
    // will fail to link if we're using old Expat.
  auto p = XML_ParserCreate(nullptr);
  ASSERT_TRUE(p != nullptr) << "Failed to create XML parser";
  // OBJGEN1-627: Lower XML parser attack thresholds aggressively.
  ASSERT_EQ(XML_TRUE, XML_SetBillionLaughsAttackProtectionActivationThreshold(p, 1024UL * 1024UL)) << "Failed to set XML parser attack protection threshold";
  ASSERT_EQ(XML_TRUE, XML_SetBillionLaughsAttackProtectionMaximumAmplification(p, 10.0)) << "Failed to set XML parser attack protection amplification";
  XML_ParserFree(p);
}

