package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	AssessmentSuccessRatioBasisAllAccepted = "all_accepted_probes"
	AssessmentReasonBackfillCounted        = "backfill_observations_counted"
	AssessmentRatioReasonLive              = "success_ratio_counts_all_accepted_live_probes"
	AssessmentRatioReasonLiveAndBackfill   = "success_ratio_counts_all_accepted_live_and_backfill_probes"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	input = NormalizeProbeInput(input)
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	if input.Backfill {
		payload = fmt.Sprintf("%s|backfill|%s", payload, input.BackfillReason)
	}
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ValidateBackfillSchedule(input ProbeInput, lease NodeLease, plan RolloutPlan, now time.Time) error {
	observedAt := input.ObservedAt.UTC()
	if !input.Backfill {
		return nil
	}
	earliest := lease.ClaimedAt.UTC().Add(-MaxBackfillObservationAge)
	switch {
	case observedAt.Before(earliest):
		return NewError(CodeBackfillRejected, "backfill observation is older than the allowed window", map[string]any{
			"observed_at":      observedAt,
			"earliest_allowed": earliest,
			"reason":           input.BackfillReason,
		})
	case !observedAt.Before(lease.ClaimedAt.UTC()):
		return NewError(CodeBackfillRejected, "backfill observation must predate lease claim", map[string]any{
			"observed_at": observedAt,
			"claimed_at":  lease.ClaimedAt,
			"reason":      input.BackfillReason,
		})
	case !now.Before(lease.ExpiresAt.UTC()):
		return NewError(CodeLeaseExpired, "lease has expired before backfill submission", map[string]any{
			"lease_id":   lease.ID,
			"expires_at": lease.ExpiresAt,
		})
	case plan.ActivatedAt != nil && observedAt.Before(plan.ActivatedAt.UTC()):
		return NewError(CodeBackfillRejected, "backfill observation predates the active lease schedule", map[string]any{
			"observed_at":  observedAt,
			"activated_at": plan.ActivatedAt,
			"reason":       input.BackfillReason,
		})
	default:
		return nil
	}
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	reviewRequired := false
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
		reviewRequired = true
	}
	probeCount := len(probes)
	successCount := 0
	backfillProbeCount := 0
	backfillSuccessCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	backfillReasonSet := make(map[BackfillReason]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Backfill {
			backfillProbeCount++
			backfillReasonSet[probe.BackfillReason] = struct{}{}
		}
		if probe.Success {
			successCount++
			if probe.Backfill {
				backfillSuccessCount++
			}
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
				reviewRequired = true
			}
		}
	}
	backfillReasons := make([]BackfillReason, 0, len(backfillReasonSet))
	for reason := range backfillReasonSet {
		backfillReasons = append(backfillReasons, reason)
	}
	sort.Slice(backfillReasons, func(i, j int) bool {
		return backfillReasons[i] < backfillReasons[j]
	})
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
		reviewRequired = true
	}
	successRatio := 0.0
	successRatioReason := ""
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
		successRatioReason = AssessmentRatioReasonLive
	}
	if backfillProbeCount > 0 {
		reasons[AssessmentReasonBackfillCounted] = struct{}{}
		successRatioReason = AssessmentRatioReasonLiveAndBackfill
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
		reviewRequired = true
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
		reviewRequired = true
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if reviewRequired {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:                   id,
		PlanID:               plan.ID,
		RevisionID:           revision.ID,
		Verdict:              verdict,
		Reasons:              reasonList,
		ProbeCount:           probeCount,
		SuccessCount:         successCount,
		SuccessRatio:         successRatio,
		MaximumLatencyMS:     maximumLatency,
		BackfillProbeCount:   backfillProbeCount,
		BackfillSuccessCount: backfillSuccessCount,
		BackfillReasons:      backfillReasons,
		SuccessRatioBasis:    AssessmentSuccessRatioBasisAllAccepted,
		SuccessRatioReason:   successRatioReason,
		CreatedAt:            now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
