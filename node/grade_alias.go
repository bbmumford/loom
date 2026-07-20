/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import "github.com/bbmumford/loom/grade"

// Re-export Grade types from mesh/grade so existing node code compiles
// without adding grade. prefix to every reference.
type Grade = grade.Grade

const (
	GradeF = grade.GradeF
	GradeC = grade.GradeC
	GradeB = grade.GradeB
	GradeA = grade.GradeA
)

var (
	SessionGrade        = grade.SessionGrade
	GradeFromCapabilities = grade.GradeFromCapabilities
	ProtocolGrade       = grade.ProtocolGrade
	MaxSupportedGrade   = grade.MaxSupportedGrade
)

type OperationClass = grade.OperationClass

const (
	OpClassBulk     = grade.OpClassBulk
	OpClassStandard = grade.OpClassStandard
	OpClassRealtime = grade.OpClassRealtime
	OpClassCritical = grade.OpClassCritical
)
