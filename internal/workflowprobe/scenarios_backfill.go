package workflowprobe

import (
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

// runBackfillObservation verifies the compensated-observation ("补传") path:
// controlled marker and reason, identity retained on lease/probe/assessment,
// stable ratio disclosure, and rejection of misuse, replay, and out-of-window
// hand-ins. Ordinary probes are exercised by the other scenarios.
func runBackfillObservation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	t0 := harness.clock.Now()

	for _, name := range []string{"resolver-bf-01", "resolver-bf-02", "resolver-bf-03", "resolver-bf-04", "resolver-bf-05", "resolver-bf-06"} {
		if _, err := enrollNode(harness, name, "central"); err != nil {
			return err
		}
	}

	// Plan A: one live probe and one accepted backfill probe.
	planA, revisionA, err := createActivePlan(harness, "bf-accept.example", 6001, 2, 1.0, 100)
	if err != nil {
		return err
	}
	groupA, err := allocate(harness, planA.ID, "bf-lease-a-0001", "central", 2)
	if err != nil {
		return err
	}
	liveLease, backfillLease := groupA.LeaseIDs[0], groupA.LeaseIDs[1]

	// Plan B: leases reserved for rejection cases.
	planB, _, err := createActivePlan(harness, "bf-reject.example", 7001, 4, 0.5, 100)
	if err != nil {
		return err
	}
	groupB, err := allocate(harness, planB.ID, "bf-lease-b-0001", "central", 4)
	if err != nil {
		return err
	}
	claimedLease, lateLease, expiredLiveLease, releasedLease :=
		groupB.LeaseIDs[0], groupB.LeaseIDs[1], groupB.LeaseIDs[2], groupB.LeaseIDs[3]

	// Ordinary live probe keeps its original behavior and carries no marker.
	live, err := submitProbe(harness, liveLease, "bf-live-0001", true, revisionA.Serial, 10)
	if err != nil {
		return err
	}
	if live.Backfill || live.BackfillReason != "" {
		return ensure(false, "ordinary probe must not carry a backfill identity")
	}

	// A released lease can never accept a backfill.
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+releasedLease+"/release", nil, &released); err != nil {
		return err
	}

	// --- Marker misuse and malformed markers (all rejected, no state change). ---
	_, err = submitProbeDetailed(harness, claimedLease, domain.ProbeInput{
		RequestKey: "bf-bad-reason-01", Success: true, ObservedSerial: 7001,
		LatencyMS: 10, ObservedAt: t0.Add(-2 * time.Minute),
		Backfill: true, BackfillReason: "",
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	_, err = submitProbeDetailed(harness, claimedLease, domain.ProbeInput{
		RequestKey: "bf-bad-reason-02", Success: true, ObservedSerial: 7001,
		LatencyMS: 10, ObservedAt: t0.Add(-2 * time.Minute),
		Backfill: true, BackfillReason: "weather",
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	_, err = submitProbeDetailed(harness, claimedLease, domain.ProbeInput{
		RequestKey: "bf-bad-reason-03", Success: true, ObservedSerial: 7001,
		LatencyMS: 10, ObservedAt: t0.Add(-2 * time.Minute),
		Backfill: false, BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	// Observation must genuinely predate the claim.
	_, err = submitBackfill(harness, claimedLease, "bf-misuse-0001", true, 7001, 10,
		t0, domain.BackfillReasonNodeOffline)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillMisused)); err != nil {
		return err
	}
	// Observation older than the bounded lead window.
	_, err = submitBackfill(harness, claimedLease, "bf-old-0000001", true, 7001, 10,
		t0.Add(-20*time.Minute), domain.BackfillReasonNodeOffline)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillWindow)); err != nil {
		return err
	}

	// A valid backfill while the lease is still claimed is accepted.
	claimedBackfill, err := submitBackfill(harness, claimedLease, "bf-claimed-001", true, 7001, 12,
		t0.Add(-3*time.Minute), domain.BackfillReasonNetworkPartition)
	if err != nil {
		return err
	}
	if !claimedBackfill.Backfill || claimedBackfill.BackfillReason != domain.BackfillReasonNetworkPartition {
		return ensure(false, "accepted backfill identity was not preserved on the probe")
	}

	// Node reconnects after lease expiry but inside the late window.
	harness.clock.Advance(15 * time.Minute) // now t0+15m; leases expired at t0+10m.
	accepted, err := submitBackfill(harness, backfillLease, "bf-accept-001", true, revisionA.Serial, 14,
		t0.Add(-5*time.Minute), domain.BackfillReasonNodeOffline)
	if err != nil {
		return err
	}
	if !accepted.Backfill || accepted.BackfillReason != domain.BackfillReasonNodeOffline {
		return ensure(false, "expired-lease backfill identity was not preserved")
	}
	if !accepted.ObservedAt.Equal(t0.Add(-5 * time.Minute)) {
		return ensure(false, "backfill observed_at was not preserved")
	}

	// Backfill is a one-off hand-in: neither identical replay nor a fresh key
	// may add a second compensated observation to the same lease.
	_, err = submitBackfill(harness, backfillLease, "bf-accept-001", true, revisionA.Serial, 14,
		t0.Add(-5*time.Minute), domain.BackfillReasonNodeOffline)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillReplay)); err != nil {
		return err
	}
	_, err = submitBackfill(harness, backfillLease, "bf-accept-002", true, revisionA.Serial, 14,
		t0.Add(-5*time.Minute), domain.BackfillReasonNodeOffline)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillReplay)); err != nil {
		return err
	}

	// Released lease still rejects a backfill.
	_, err = submitBackfill(harness, releasedLease, "bf-released-01", true, 7001, 10,
		t0.Add(-2*time.Minute), domain.BackfillReasonNodeOffline)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}

	// Assessment must disclose how compensated results enter the ratio.
	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planA.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "bf-assess-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.ProbeCount != 2 || assessment.SuccessCount != 2 {
		return ensure(false, "assessment did not count both probes")
	}
	if assessment.BackfillProbeCount != 1 || assessment.BackfillSuccessCount != 1 || assessment.LiveProbeCount != 1 {
		return ensure(false, "assessment backfill/live breakdown is incorrect")
	}
	if !assessment.BackfillIncludedInRatio || assessment.SuccessRatio != 1.0 {
		return ensure(false, "backfill inclusion flag or ratio is incorrect")
	}
	if !containsReason(assessment.Reasons, domain.ReasonBackfillIncluded) {
		return ensure(false, "assessment reasons must stably disclose backfill inclusion")
	}
	if assessment.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "compensated evidence must keep the verdict under review")
	}

	// Beyond the late window the backfill is rejected even with valid contents.
	harness.clock.Advance(30 * time.Minute) // now t0+45m; deadline was t0+40m.
	_, err = submitBackfill(harness, lateLease, "bf-late-00001", true, 7001, 10,
		t0.Add(-2*time.Minute), domain.BackfillReasonNodeOffline)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillWindow)); err != nil {
		return err
	}
	// An ordinary (non-backfill) probe on an expired lease keeps failing as
	// lease_expired, unaffected by the new path.
	_, err = submitProbeDetailed(harness, expiredLiveLease, domain.ProbeInput{
		RequestKey: "bf-live-late-1", Success: true, ObservedSerial: 7001,
		LatencyMS: 10, ObservedAt: harness.clock.Now(),
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}
	return nil
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
