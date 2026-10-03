package workflowprobe

import (
	"fmt"
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runConsistencyAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-assess-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-assess-02", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "assess.example", 3001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "assess-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("assess-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	var consistent domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &consistent); err != nil {
		return err
	}
	if consistent.Verdict != domain.VerdictConsistent || consistent.ProbeCount != 2 {
		return ensure(false, "consistent assessment did not include both probes")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != consistent.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != consistent.ID {
		return ensure(false, "latest assessment does not match")
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	); err != nil {
		return err
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &review); err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "mismatched serial did not require review")
	}

	backfillPlan, backfillRevision, err := createActivePlan(harness, "backfill.example", 5001, 1, 1, 20)
	if err != nil {
		return err
	}
	harness.clock.Advance(3 * time.Minute)
	backfillGroup, err := allocate(harness, backfillPlan.ID, "backfill-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	backfillObservedAt := harness.clock.Now().Add(-2 * time.Minute)
	if _, err := submitProbeInput(harness, backfillGroup.LeaseIDs[0], domain.ProbeInput{
		RequestKey:     "backfill-probe-0001",
		Success:        true,
		ObservedSerial: backfillRevision.Serial,
		LatencyMS:      8,
		ObservedAt:     backfillObservedAt,
		Backfill:       true,
		BackfillReason: domain.BackfillReasonNodeOffline,
	}); err != nil {
		return err
	}
	var backfillAssessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+backfillPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "backfill-assess-0001",
	}, &backfillAssessment); err != nil {
		return err
	}
	if backfillAssessment.Verdict != domain.VerdictConsistent ||
		backfillAssessment.ProbeCount != 1 ||
		backfillAssessment.BackfillProbeCount != 1 ||
		backfillAssessment.BackfillSuccessCount != 1 ||
		backfillAssessment.SuccessRatio != 1 ||
		!containsReason(backfillAssessment.Reasons, "backfill_observations_counted") ||
		len(backfillAssessment.BackfillReasons) != 1 ||
		backfillAssessment.BackfillReasons[0] != domain.BackfillReasonNodeOffline ||
		backfillAssessment.SuccessRatioBasis != "all_accepted_probes" ||
		backfillAssessment.SuccessRatioReason != "success_ratio_counts_all_accepted_live_and_backfill_probes" {
		return ensure(false, "assessment did not expose the backfill success ratio basis")
	}
	return nil
}

func containsReason(reasons []string, wanted string) bool {
	for _, reason := range reasons {
		if reason == wanted {
			return true
		}
	}
	return false
}
