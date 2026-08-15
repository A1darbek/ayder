#ifndef RAMFORGE_HA_FAULT_INJECTION_H
#define RAMFORGE_HA_FAULT_INJECTION_H

/*
 * Test-only transport fault injection.  The production default is disabled.
 * Both RF_HA_TEST_FAULTS=1 and RF_HA_TEST_PARTITION_FILE=<path> are required.
 * The control file contains one line: "cut_mask=<integer>".  A link is cut
 * when exactly one endpoint's membership-index bit is present in cut_mask.
 */
int HA_test_link_is_partitioned(int local_index, int peer_index);

#endif
