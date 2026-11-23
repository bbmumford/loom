/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package bidding

import (
	"testing"
	"time"

	task "github.com/bbmumford/loom/core/task"
)

// ScoreRule awards the highest score at or above the minimum, answering every
// candidate and the rule's name; a tie on the score goes to the bid received
// first and is marked ambiguous.
func TestScoreRuleAwardsTheHighestScoreAndMarksATie(t *testing.T) {
	now := time.Unix(1_000, 0)
	policy := BidPolicy{Weights: BidWeights{Capacity: 1}, MinScore: 2}
	rule := ScoreRule{Policy: policy}
	offer := task.TaskOffer{Task: &task.Task{ID: "t1"}}

	bids := []task.Bid{
		{TaskID: "t1", NodeID: "low", CapacityScore: 1, ReceivedAt: now},
		{TaskID: "t1", NodeID: "high", CapacityScore: 5, ReceivedAt: now.Add(time.Second)},
		{TaskID: "t1", NodeID: "mid", CapacityScore: 3, ReceivedAt: now},
	}
	d, err := rule.Award(offer, bids, AwardContext{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if d.Winner == nil || d.Winner.NodeID != "high" || d.Ambiguous || d.Rule != ScoreRuleName || len(d.Candidates) != 3 {
		t.Fatalf("decision = %+v, want high, unambiguous, three candidates", d)
	}

	tied := []task.Bid{
		{TaskID: "t1", NodeID: "b", CapacityScore: 4, ReceivedAt: now.Add(time.Second)},
		{TaskID: "t1", NodeID: "a", CapacityScore: 4, ReceivedAt: now.Add(2 * time.Second)},
	}
	d, err = rule.Award(offer, tied, AwardContext{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if d.Winner == nil || d.Winner.NodeID != "b" || !d.Ambiguous {
		t.Fatalf("tied decision = %+v, want the first received (b), ambiguous", d)
	}
}

// No bid at the minimum answers no winner with a reason, and no bids at all is
// an error, so a gateway never reads an empty decision as an award.
func TestScoreRuleAwardsNothingBelowTheMinimum(t *testing.T) {
	now := time.Unix(1_000, 0)
	rule := ScoreRule{Policy: BidPolicy{Weights: BidWeights{Capacity: 1}, MinScore: 9}}
	offer := task.TaskOffer{Task: &task.Task{ID: "t1"}}
	d, err := rule.Award(offer, []task.Bid{{TaskID: "t1", NodeID: "n", CapacityScore: 2, ReceivedAt: now}}, AwardContext{Now: now})
	if err != nil || d.Winner != nil || d.Reason == "" || len(d.Candidates) != 1 {
		t.Fatalf("decision = %+v, err %v; want no winner, a reason and the one candidate", d, err)
	}
	if _, err := rule.Award(offer, nil, AwardContext{Now: now}); err == nil {
		t.Fatal("an award over no bids answered no error")
	}
}

// A recent winner is penalised by the diversity weight, so the same capacity
// from a node that did not win recently scores higher.
func TestScoreRuleWeighsARecentWinnerDown(t *testing.T) {
	now := time.Unix(1_000, 0)
	rule := ScoreRule{Policy: BidPolicy{Weights: BidWeights{Capacity: 1, Diversity: 1}, MinScore: 0}}
	offer := task.TaskOffer{Task: &task.Task{ID: "t1"}}
	bids := []task.Bid{
		{TaskID: "t1", NodeID: "recent", CapacityScore: 3, ReceivedAt: now},
		{TaskID: "t1", NodeID: "fresh", CapacityScore: 3, ReceivedAt: now.Add(time.Second)},
	}
	d, err := rule.Award(offer, bids, AwardContext{Now: now, RecentWinner: func(n string) bool { return n == "recent" }})
	if err != nil || d.Winner == nil || d.Winner.NodeID != "fresh" {
		t.Fatalf("decision = %+v, err %v; want fresh", d, err)
	}
}
