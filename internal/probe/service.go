package probe

import (
	"context"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type LeaseExpirer interface {
	ExpireDue(context.Context) (int, error)
}

type Service struct {
	store   *store.Store
	clock   platform.Clock
	ids     platform.IDGenerator
	expirer LeaseExpirer
}

func New(
	stateStore *store.Store,
	clock platform.Clock,
	ids platform.IDGenerator,
	expirer LeaseExpirer,
) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids, expirer: expirer}
}

func (s *Service) Submit(
	ctx context.Context,
	leaseID string,
	input domain.ProbeInput,
) (domain.Probe, error) {
	now := s.clock.Now().UTC()
	input = domain.NormalizeProbeInput(input)
	if err := domain.ValidateProbeInput(input, now); err != nil {
		return domain.Probe{}, err
	}
	if _, err := s.expirer.ExpireDue(ctx); err != nil {
		return domain.Probe{}, err
	}
	fingerprint := domain.ProbeFingerprint(leaseID, input)
	var result domain.Probe
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReported && lease.ProbeID != "" {
			existing, found := state.Probes[lease.ProbeID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease references a missing probe", map[string]any{
					"probe_id": lease.ProbeID,
				})
			}
			// A compensated observation is a one-off hand-in: it can never be
			// replayed, even when the request key and payload match. Live probes
			// keep their original same-key/same-content idempotent replay.
			if existing.Backfill || input.Backfill {
				return domain.NewError(domain.CodeBackfillReplay, "a backfilled probe cannot be replayed", map[string]any{
					"lease_id":    leaseID,
					"request_key": input.RequestKey,
				})
			}
			if existing.PayloadFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different probe content", map[string]any{
					"lease_id":    leaseID,
					"request_key": input.RequestKey,
				})
			}
			result = existing
			return nil
		}

		// Released leases can never accept a probe; expired leases may accept a
		// backfill only within the bounded late window.
		if lease.State == domain.LeaseReleased {
			return domain.NewError(domain.CodeLeaseNotClaimed, "lease is not claimed", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		if lease.State != domain.LeaseClaimed && lease.State != domain.LeaseExpired {
			return domain.NewError(domain.CodeLeaseNotClaimed, "lease is not claimed", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}

		plan, ok := state.Plans[lease.PlanID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing plan", map[string]any{
				"plan_id": lease.PlanID,
			})
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active to store probes", map[string]any{
				"plan_id": plan.ID,
				"state":   plan.State,
			})
		}

		observedAt := input.ObservedAt.UTC()
		if input.Backfill {
			// Marker misuse: the compensated marker is valid only when the
			// observation genuinely predates the lease claim.
			if !observedAt.Before(lease.ClaimedAt.UTC()) {
				return domain.NewError(domain.CodeBackfillMisused, "backfill observation must predate the lease claim", map[string]any{
					"lease_id":    leaseID,
					"claimed_at":  lease.ClaimedAt,
					"observed_at": observedAt,
				})
			}
			// The observation must fit the leased plan window: no earlier than
			// the bounded lead before claim, and no later than lease expiry.
			if observedAt.Before(lease.ClaimedAt.UTC().Add(-domain.MaxBackfillLead)) {
				return domain.NewError(domain.CodeBackfillWindow, "backfill observation is older than the allowed lead window", map[string]any{
					"lease_id":    leaseID,
					"observed_at": observedAt,
					"lead":        domain.MaxBackfillLead.String(),
				})
			}
			if observedAt.After(lease.ExpiresAt.UTC()) {
				return domain.NewError(domain.CodeBackfillWindow, "backfill observation must fall within the lease window", map[string]any{
					"lease_id":   leaseID,
					"expires_at": lease.ExpiresAt,
				})
			}
			// Hand-in is accepted after expiry only within the late window.
			if lease.State == domain.LeaseExpired && now.After(lease.ExpiresAt.UTC().Add(domain.MaxBackfillLateWindow)) {
				return domain.NewError(domain.CodeBackfillWindow, "backfill hand-in is later than the allowed window", map[string]any{
					"lease_id": leaseID,
					"deadline": lease.ExpiresAt.UTC().Add(domain.MaxBackfillLateWindow),
				})
			}
		} else {
			// Live probes keep their original behavior: claimed and unexpired.
			if lease.State != domain.LeaseClaimed || !now.Before(lease.ExpiresAt) {
				return domain.NewError(domain.CodeLeaseExpired, "lease has expired", map[string]any{
					"lease_id":   leaseID,
					"expires_at": lease.ExpiresAt,
				})
			}
		}

		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		result = domain.Probe{
			ID:                 s.ids.New("prb"),
			LeaseID:            leaseID,
			PlanID:             lease.PlanID,
			NodeID:             lease.NodeID,
			RequestKey:         input.RequestKey,
			Success:            input.Success,
			ObservedSerial:     input.ObservedSerial,
			LatencyMS:          input.LatencyMS,
			ObservedAt:         observedAt,
			StoredAt:           now,
			PayloadFingerprint: fingerprint,
			Backfill:           input.Backfill,
			BackfillReason:     input.BackfillReason,
		}
		state.Probes[result.ID] = result
		lease.State = domain.LeaseReported
		if lease.FinishedAt == nil {
			finishedAt := now
			lease.FinishedAt = &finishedAt
		}
		lease.ProbeID = result.ID
		lease.Backfill = input.Backfill
		lease.BackfillReason = input.BackfillReason
		state.Leases[leaseID] = lease
		// A backfill against an expired lease reconciles a node that expiry
		// already released; only touch a node still tied to this lease.
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		auditAttributes := map[string]any{
			"lease_id": leaseID,
			"plan_id":  lease.PlanID,
		}
		if input.Backfill {
			auditAttributes["backfill"] = true
			auditAttributes["backfill_reason"] = string(input.BackfillReason)
		}
		store.AddAudit(state, s.ids, "probe", result.ID, "stored", now, auditAttributes)
		return nil
	})
	return result, err
}
