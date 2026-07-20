/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	lad "github.com/bbmumford/ledger"
)

// MessageType identifies the kind of gossip message.
type MessageType uint8

const (
	MsgPush MessageType = iota
	MsgPull
	MsgPullResponse
)

// GossipMessage is the envelope for all gossip traffic.
type GossipMessage struct {
	Type    MessageType `json:"type"`
	Payload []byte      `json:"payload"`
}

// GossipPush contains a batch of new ledger records.
type GossipPush struct {
	SenderNodeID string       `json:"senderNodeId"`
	Records      []lad.Record `json:"records"`
}

// GossipPull requests missing records.
type GossipPull struct {
	SenderNodeID string              `json:"senderNodeId"`
	Watermark    lad.CausalWatermark `json:"watermark"`
}

// GossipPullResponse contains the requested records.
type GossipPullResponse struct {
	Records []lad.Record `json:"records"`
}
