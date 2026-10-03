package workflowprobe

import (
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runProbeIngestion() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-probe-01", "central"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-probe-02", "central"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-probe-03", "central"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-probe-04", "central"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "probe.example", 2049, 3, 0.34, 50)
	if err != nil {
		return err
	}
	harness.clock.Advance(3 * time.Minute)
	group, err := allocate(harness, plan.ID, "probe-lease-0001", "central", 3)
	if err != nil {
		return err
	}

	leaseID := group.LeaseIDs[0]
	first, err := submitProbe(harness, leaseID, "probe-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if !first.Success || first.ObservedSerial != revision.Serial || first.Backfill {
		return ensure(false, "probe values were not preserved")
	}
	replay, err := submitProbe(harness, leaseID, "probe-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "probe replay returned a different item")
	}
	_, err = submitProbe(harness, leaseID, "probe-request-0001", true, revision.Serial, 25)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/release", nil, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	backfillLeaseID := group.LeaseIDs[1]
	observedAt := harness.clock.Now().Add(-2 * time.Minute)
	backfill, err := submitProbeInput(harness, backfillLeaseID, domain.ProbeInput{
		RequestKey:     "probe-request-0002",
		Success:        true,
		ObservedSerial: revision.Serial,
		LatencyMS:      18,
		ObservedAt:     observedAt,
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err != nil {
		return err
	}
	if !backfill.Backfill || backfill.BackfillReason != domain.BackfillReasonNodeOffline || !backfill.ObservedAt.Equal(observedAt) {
		return ensure(false, "backfill identity was not preserved")
	}
	backfillReplay, err := submitProbeInput(harness, backfillLeaseID, domain.ProbeInput{
		RequestKey:     "probe-request-0002",
		Success:        true,
		ObservedSerial: revision.Serial,
		LatencyMS:      18,
		ObservedAt:     observedAt,
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err != nil {
		return err
	}
	if backfillReplay.ID != backfill.ID {
		return ensure(false, "backfill replay returned a different item")
	}
	_, err = submitProbeInput(harness, backfillLeaseID, domain.ProbeInput{
		RequestKey:     "probe-request-0003",
		Success:        true,
		ObservedSerial: revision.Serial,
		LatencyMS:      18,
		ObservedAt:     observedAt,
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	misusePlan, misuseRevision, err := createActivePlan(harness, "misuse.example", 2050, 3, 1, 50)
	if err != nil {
		return err
	}
	misuseGroup, err := allocate(harness, misusePlan.ID, "probe-lease-0003", "central", 3)
	if err != nil {
		return err
	}
	_, err = submitProbeInput(harness, misuseGroup.LeaseIDs[0], domain.ProbeInput{
		RequestKey:     "probe-request-0004",
		Success:        true,
		ObservedSerial: misuseRevision.Serial,
		LatencyMS:      12,
		ObservedAt:     harness.clock.Now().Add(-time.Minute),
		Backfill:       true,
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	_, err = submitProbeInput(harness, misuseGroup.LeaseIDs[0], domain.ProbeInput{
		RequestKey:     "probe-request-0005",
		Success:        true,
		ObservedSerial: misuseRevision.Serial,
		LatencyMS:      12,
		ObservedAt:     harness.clock.Now(),
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillRejected)); err != nil {
		return err
	}
	_, err = submitProbeInput(harness, misuseGroup.LeaseIDs[1], domain.ProbeInput{
		RequestKey:     "probe-request-0006",
		Success:        true,
		ObservedSerial: misuseRevision.Serial,
		LatencyMS:      12,
		ObservedAt:     harness.clock.Now().Add(-20 * time.Minute),
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillRejected)); err != nil {
		return err
	}
	preActivationObservedAt := misusePlan.ActivatedAt.Add(-time.Minute)
	_, err = submitProbeInput(harness, misuseGroup.LeaseIDs[2], domain.ProbeInput{
		RequestKey:     "probe-request-0007",
		Success:        true,
		ObservedSerial: misuseRevision.Serial,
		LatencyMS:      12,
		ObservedAt:     preActivationObservedAt,
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeBackfillRejected)); err != nil {
		return err
	}
	_, err = submitProbeInput(harness, misuseGroup.LeaseIDs[1], domain.ProbeInput{
		RequestKey:     "probe-request-0008",
		Success:        true,
		ObservedSerial: misuseRevision.Serial,
		LatencyMS:      12,
		ObservedAt:     harness.clock.Now().Add(-time.Minute),
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	harness.clock.Advance(601 * time.Second)
	_, err = submitProbeInput(harness, misuseGroup.LeaseIDs[1], domain.ProbeInput{
		RequestKey:     "probe-request-0009",
		Success:        true,
		ObservedSerial: misuseRevision.Serial,
		LatencyMS:      12,
		ObservedAt:     misuseGroup.CreatedAt.Add(-time.Minute),
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}
	return nil
}
