/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package bidding

import (
	"errors"
	"sort"
	"time"

	task "github.com/bbmumford/loom/core/task"
)

// AwardDecision is an award rule's answer over the bids one offer collected.
//
// It carries the whole selection, not only its value: the winning bid, the rule
// that decided, every candidate the rule saw and whether the deciding
// comparison tied at the top, so a consumer recording an award records why it
// went where it did. Winner is nil when no bid is eligible, and Reason then
// says why.
type AwardDecision struct {
	Winner     *task.Bid  `json:"winner,omitempty"`
	Rule       string     `json:"rule"`
	Candidates []task.Bid `json:"candidates"`
	Ambiguous  bool       `json:"ambiguous"`
	Reason     string     `json:"reason,omitempty"`
}

// AwardContext is what the gateway knows at the moment an award is decided.
type AwardContext struct {
	// Now is the gateway's clock at the decision.
	Now time.Time
	// RecentWinner reports whether node won an award within the gateway's
	// recent-win window. Nil reports no recent winner.
	RecentWinner func(node string) bool
}

// AwardRule decides which of an offer's bids wins. The gateway calls it once
// an offer's bid window closes, with every bid the window collected (the latest
// from each node), and again for each bid that arrives later while the offer
// is still open and nothing has won. A job kind supplies its own rule; ScoreRule
// is the gateway's rule when none is configured.
//
// Award must not keep or change bids or offer; an error refuses the offer's
// award for this pass, and the gateway treats it as no eligible bid.
type AwardRule interface {
	Award(offer task.TaskOffer, bids []task.Bid, at AwardContext) (AwardDecision, error)
}

// ScoreRuleName names ScoreRule in an AwardDecision.
const ScoreRuleName = "loom.score"

// ScoreRule awards the bid BidPolicy.Evaluate scores highest, among the bids
// scoring at least Policy.MinScore. A tie on the score goes to the bid received
// first, then to the lower node id, and the decision is marked ambiguous.
type ScoreRule struct {
	Policy BidPolicy
}

// errNoBids refuses an award asked over no bids.
var errNoBids = errors.New("bidding: no bids to award")

// Award implements AwardRule.
func (r ScoreRule) Award(offer task.TaskOffer, bids []task.Bid, at AwardContext) (AwardDecision, error) {
	decision := AwardDecision{Rule: ScoreRuleName, Candidates: append([]task.Bid(nil), bids...)}
	if len(bids) == 0 {
		return decision, errNoBids
	}
	type scored struct {
		bid   task.Bid
		score float64
	}
	var eligible []scored
	for _, b := range bids {
		recent := at.RecentWinner != nil && at.RecentWinner(b.NodeID)
		s := r.Policy.Evaluate(b, at.Now, recent)
		if s >= r.Policy.MinScore {
			eligible = append(eligible, scored{bid: b, score: s})
		}
	}
	if len(eligible) == 0 {
		decision.Reason = "no bid scores at least the policy's minimum"
		return decision, nil
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		a, b := eligible[i], eligible[j]
		if a.score != b.score {
			return a.score > b.score
		}
		if !a.bid.ReceivedAt.Equal(b.bid.ReceivedAt) {
			return a.bid.ReceivedAt.Before(b.bid.ReceivedAt)
		}
		return a.bid.NodeID < b.bid.NodeID
	})
	winner := eligible[0].bid
	decision.Winner = &winner
	decision.Ambiguous = len(eligible) > 1 && eligible[1].score == eligible[0].score
	return decision, nil
}
