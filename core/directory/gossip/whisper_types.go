/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

// Re-export Whisper types into the gossip namespace so existing consumers
// (sync.go, stream_gossip.go, mesh/node/*.go) continue to compile.
// These aliases will be removed when consumers import whisper directly.

import "github.com/bbmumford/whisper"

// --- Types ---

type GossipResult = whisper.GossipResult
type ExchangeMeta = whisper.ExchangeMeta
type ConnectionOffer = whisper.ConnectionOffer
type DeltaTracker = whisper.DeltaTracker
type DeltaSyncMeta = whisper.DeltaSyncMeta
type DeltaPeerStats = whisper.DeltaPeerStats
type AdaptiveInterval = whisper.AdaptiveInterval
type BackpressureMonitor = whisper.BackpressureMonitor
type PeerBackoff = whisper.PeerBackoff
type Hypercube = whisper.Hypercube
type PEXEntry = whisper.PEXEntry
type PEXRequest = whisper.PEXRequest
type PEXResponse = whisper.PEXResponse
type PEXRateLimiter = whisper.PEXRateLimiter
type PEXManager = whisper.PEXManager
type NetworkRTTMeasurer = whisper.NetworkRTTMeasurer
type RumorConfig = whisper.RumorConfig
type RumorTracker = whisper.RumorTracker
type RumorPusher = whisper.RumorPusher
type RumorPeerConn = whisper.RumorPeerConn
type RumorIDFunc = whisper.RumorIDFunc
type RumorMessage = whisper.RumorMessage

// --- Constructors ---

var NewDeltaTracker = whisper.NewDeltaTracker
var NewDeltaTrackerWithInterval = whisper.NewDeltaTrackerWithInterval
var NewAdaptiveInterval = whisper.NewAdaptiveInterval
var NewBackpressureMonitor = whisper.NewBackpressureMonitor
var NewBackpressureMonitorWithThreshold = whisper.NewBackpressureMonitorWithThreshold
var NewPeerBackoff = whisper.NewPeerBackoff
var NewHypercube = whisper.NewHypercube
var NewPEXRateLimiter = whisper.NewPEXRateLimiter
var NewPEXManager = whisper.NewPEXManager
var NewNetworkRTTMeasurer = whisper.NewNetworkRTTMeasurer
var NewRumorTracker = whisper.NewRumorTracker
var NewRumorPusher = whisper.NewRumorPusher

// --- Functions ---

var SignPEXEntry = whisper.SignPEXEntry
var VerifyPEXEntry = whisper.VerifyPEXEntry
var IsStalePEXEntry = whisper.IsStalePEXEntry
var ShouldInitiateDial = whisper.ShouldInitiateDial
var WriteDigestProbe = whisper.WriteDigestProbe
var ReadDigestBody = whisper.ReadDigestBody
var WriteRumor = whisper.WriteRumor
var ReadRumorBody = whisper.ReadRumorBody
var HandleRumorFrame = whisper.HandleRumorFrame
var ServiceMeshRumorConfig = whisper.ServiceMeshRumorConfig
var AgentMeshRumorConfig = whisper.AgentMeshRumorConfig

// --- Constants ---

const DefaultBackpressureThreshold = whisper.DefaultBackpressureThreshold
const DefaultBackpressureMaxBackoff = whisper.DefaultBackpressureMaxBackoff
const DefaultBackpressureInitialBackoff = whisper.DefaultBackpressureInitialBackoff
const PEXMaxAge = whisper.PEXMaxAge
const PEXInterval = whisper.PEXInterval
const PEXMaxEntriesPerExchange = whisper.PEXMaxEntriesPerExchange
const PEXMaxKnownPeers = whisper.PEXMaxKnownPeers
const PEXMaxRateLimitEntries = whisper.PEXMaxRateLimitEntries

// Wire format constants — re-exported from Whisper.
const digestMagic = whisper.DigestMagic
const rumorMagic = whisper.RumorMagic
const FlagDigestMatch = whisper.FlagDigestMatch
