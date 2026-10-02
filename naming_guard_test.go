/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package loom

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// TestNamingGuard fails when this module's naming surface carries an identity
// class word from barredWords, a product name or the name of a module that
// composes loom (callerWords). It walks the module root and reads:
//
//   - every file and directory name, matched on the word stems in namingStems
//     and on the product and caller names;
//   - every identifier and every comment in each hand-written .go file, test
//     files included, so a test, its helpers and its narration use the same
//     words as the code they exercise;
//   - every string literal and struct tag in each hand-written non-test .go
//     file, which is where wire keys, metric keys, log keys, error text a
//     caller matches on and scope strings live;
//   - every identifier, literal and comment in each .proto file.
//
// A test file's literals are its fixtures: a test proving that a key is refused
// has to spell that key, so they are not read. A file ast.IsGenerated reports
// is regenerated from its .proto, which is read instead. The licence header
// before the package clause is excused. Every other finding fails unless an
// entry in namingExemptions excuses it, and each entry is pinned to the exact
// number of findings it excuses, so an entry that matches nothing, or matches
// a new site, fails too. go test ./... in this module runs it, so a change that
// adds such a word fails in that change.
func TestNamingGuard(t *testing.T) {
	findings, stats, err := scanNaming(".")
	if err != nil {
		t.Fatal(err)
	}
	// A walk wired to the wrong directory reads nothing and reports nothing;
	// these floors, set from this module's size, make that walk fail instead.
	if stats.goFiles < 300 || stats.protoFiles < 2 {
		t.Fatalf("naming guard read %d .go and %d .proto files; the walk did not reach the module", stats.goFiles, stats.protoFiles)
	}

	open, used := applyExemptions(findings, namingExemptions)
	for _, f := range open {
		t.Errorf("%s:%d: %s %q in %s", f.file, f.line, f.kind, f.word, clipText(f.text))
	}
	for _, msg := range exemptionErrors(namingExemptions, used) {
		t.Error(msg)
	}
	excused := 0
	for _, n := range used {
		excused += n
	}
	t.Logf("naming guard: %d .go (%d generated, skipped) and %d .proto files; %d findings; %d excused under %d exemptions",
		stats.goFiles, stats.generated, stats.protoFiles, len(open), excused, len(namingExemptions))
}

// TestNamingMatcherFindsWhatItGuards feeds the scanner planted sources, so a
// matcher that stops splitting words, stops reading a surface or starts
// excusing a file fails here and not by reporting a clean module.
func TestNamingMatcherFindsWhatItGuards(t *testing.T) {
	cases := []struct {
		name, file, src string
		want            []string
	}{
		{"camel-case identifier", "a.go", "package a\nfunc dialCrossTenant() {}\n", []string{"identifier tenant"}},
		{"identifier in a test file", "a_test.go", "package a\nvar customerProbe = 1\n", []string{"identifier customer"}},
		{"snake-case wire tag", "a.go", "package a\ntype T struct{ P string `json:\"org_id\"` }\n", []string{"literal org"}},
		{"kebab-case metric key", "a.go", "package a\nvar k = \"compose-task-owner-limit\"\n", []string{"literal owner"}},
		{"plural and joined words", "a.go", "package a\nvar k = \"tenants.ownership\"\n", []string{"literal tenants", "literal ownership"}},
		{"proto package", "a.proto", "syntax = \"proto3\";\npackage hstles.mesh.v1;\n", []string{"identifier hstles"}},
		{"proto field comment", "a.proto", "syntax = \"proto3\";\n// the organisation that sent it\nmessage M {}\n", []string{"comment organisation"}},
		{"product host in a literal", "a.go", "package a\nvar u = \"https://help.orbtr.io\"\n", []string{"literal help.orbtr.io"}},
		{"product namespace in a comment", "a.go", "package a\n// served as orbtr.ai.auth.Login\nvar x = 1\n", []string{"comment orbtr.ai"}},
		{"product stem in a comment", "a.go", "package a\n// served for HSTLES callers\nvar x = 1\n", []string{"comment hstles"}},
		{"product name in a comment", "a.go", "package a\n// the ORBTR platform realm\nvar x = 1\n", []string{"comment orbtr"}},
		{"product name in a test identifier", "a_test.go", "package a\nfunc TestIncludesORBTRPolicy() {}\n", []string{"identifier orbtr"}},
		{"caller names in a comment", "a.go", "package a\n// cortex model_registry and Constellation call this\nvar x = 1\n", []string{"comment cortex", "comment constellation"}},
		{"caller name in a file name", "cortex_bridge.go", "package a\n", []string{"file name cortex"}},
		{"a dependency's module path", "a.go", "package a\nimport _ \"github.com/ORBTR/aether\"\n// see github.com/ORBTR/aether/noise\n", nil},
		{"the neutral namespace", "a.go", "package a\n// served as orbtr.test.Probe\nvar x = 1\n", nil},
		{"class word in a comment", "a.go", "package a\n// the Tenant A transport\nvar x = 1\n", []string{"comment tenant"}},
		{"class word in a test comment", "a_test.go", "package a\n// a foreign org is refused\nvar x = 1\n", []string{"comment org"}},
		{"plural class word", "a.go", "package a\n// peers in two Organisations\nvar x = 1\n", []string{"comment organisations"}},
		{"test file name", "customer_probe_test.go", "package a\n", []string{"file name customer"}},
		{"joined file name", "tenancy.go", "package a\n", []string{"file name tenancy"}},
		{"licence header", "a.go", licenceText + "\npackage a\n", nil},
		{"licence header without the queries line", "a.go", "/*\n * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.\n */\npackage a\n", nil},
		{"licence text after the package clause", "a.go", "package a\n\n" + licenceText + "\nvar x = 1\n", []string{"comment hstles", "comment orbtr", "comment hstles"}},
		{"words that open with a class stem", "a.go", "package a\nvar originalOrchestratorOrigin = \"golang.org/x\"\n", nil},
		{"a DNS name ending in org", "a.go", "package a\n// see example.org for the format\nvar x = 1\n", nil},
		{"literal in a test file", "a_test.go", "package a\nvar k = \"tenant_id\"\n", nil},
		{"generated file", "a.pb.go", "// Code generated by protoc-gen-go. DO NOT EDIT.\n\npackage a\nvar tenantID = 1\n", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, f := range fileFindings(tc.file, []byte(tc.src)) {
				if !f.licence {
					got = append(got, f.kind+" "+f.word)
				}
			}
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("findings = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNamingExemptionsArePinned checks the rule that keeps namingExemptions
// from outliving its sites: an entry excusing fewer or more findings than its
// count, or carrying no reason, is an error.
func TestNamingExemptionsArePinned(t *testing.T) {
	src := "package a\nfunc f(owner string) { _ = owner }\n"
	findings := fileFindings("a.go", []byte(src))
	pinned := []namingExemption{{"a.go", "identifier", "owner", "owner", 2, "planted"}}
	open, used := applyExemptions(findings, pinned)
	if len(open) != 0 || len(exemptionErrors(pinned, used)) != 0 {
		t.Fatalf("an exact pin left open=%v errors=%v", open, exemptionErrors(pinned, used))
	}
	for _, e := range []namingExemption{
		{"a.go", "identifier", "owner", "owner", 3, "planted"},
		{"a.go", "identifier", "owner", "owner", 2, ""},
		{"b.go", "identifier", "owner", "owner", 2, "planted"},
	} {
		_, used := applyExemptions(findings, []namingExemption{e})
		if len(exemptionErrors([]namingExemption{e}, used)) == 0 {
			t.Errorf("exemption %+v passed; want an error", e)
		}
	}
}

// licenceText is the header every hand-written file opens with, as a planted
// source for the matcher test.
const licenceText = "/*\n * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.\n * Queries: licensing@hstles.com\n */"

// barredWords are the identity-class words, compared case-blind against whole
// words as wordsOf cuts them, so a camel-case, snake-case, kebab-case or dotted
// spelling is found as the word. The list is not narrowed for this module: a
// shorter list passes a module by forgetting a word, so a name the rule does
// not reach goes in namingExemptions with its reason instead.
var barredWords = map[string]bool{
	"tenant": true, "tenants": true, "tenancy": true,
	"org": true, "orgs": true,
	"organisation": true, "organisations": true, "organization": true, "organizations": true,
	"owner": true, "owners": true, "ownership": true,
	"customer": true, "customers": true,
}

// namingStems are matched at the start of each word of a file or directory
// name, which also catches a stem joined to another word in one name.
var namingStems = []string{"tenan", "org", "owner", "custom"}

// productStem is matched inside any word, so it is also found in a URL, a
// module path, an e-mail address or an environment-variable name.
const productStem = "hstles"

// callerWords are the modules that compose loom, compared case-blind against
// whole words as wordsOf cuts them: a comment or name that says which caller
// uses a thing states a caller, not what the thing does.
var callerWords = map[string]bool{"cortex": true, "constellation": true}

// productName is the platform's name, matched as a whole upper-case word. The
// lower-case spelling is the neutral namespace of FQN examples and stays; the
// name right after moduleHost is a dependency's module path, which names the
// module loom imports (aether's), and stays too.
const productName = "ORBTR"

// moduleHost is the code host a module path in an import or a comment starts
// with.
const moduleHost = "github.com/"

// productHost matches a host or namespace under either product domain, with
// any subdomain before it.
var productHost = regexp.MustCompile(`(?i)\b(?:[a-z0-9-]+\.)*orbtr\.(?:ai|io)\b`)

// licenceHeader is the copyright block that names the licensor, with or
// without its queries line. It is excused where it is a comment before the
// package clause, and nowhere else.
var licenceHeader = regexp.MustCompile(`\A/\*\n \* Copyright \(c\) \d{4} HSTLES / ORBTR Pty Ltd\. All Rights Reserved\.\n(?: \* Queries: licensing@hstles\.com\n)? \*/\z`)

// namingExemption excuses the findings of word, of one kind, in one file,
// exactly count times. For an identifier or a literal, token is the whole
// identifier or the literal as written with its quotes, and only that token is
// excused; for a comment token is empty and every comment in the file is
// counted. A new occurrence, a moved one or a removed one changes the count and
// fails the guard.
type namingExemption struct {
	file, kind, word, token string
	count                   int
	reason                  string
}

// The reasons shared by several namingExemptions entries.
const (
	scopeHolder = "the holder of a capture scope: compose.ScopeTracker's Begin, End and Owned take it as the parameter owner"
	gossipSlot  = "the loop holding a peer's gossip slot, which evictGossipOwnerLocked clears and a higher-grade session preempts"
	meshSlot    = "the session holding a peer's mesh slot; unregisterMeshSessionWithoutHook skips a session that does not hold it"
	dialClaim   = "the side of a node pair that dials first; dialOwnershipGrace bounds how long the other side waits for it"
	recordAuth  = "the node that wrote a directory record and alone may retract it with a tombstone"
	flyNetwork  = "Fly.io's own organisation object: its 6PN private network and the fdaa:<org> prefix that marks it"
)

// namingExemptions are the names the rule does not reach: each word here names
// the holder of a slot, a scope, a dial or a record, or the Fly.io object in
// flyNetwork, and never a class of principal.
var namingExemptions = []namingExemption{
	{"compose/scope.go", "identifier", "owner", "owner", 3, scopeHolder},
	{"compose/scope.go", "comment", "owner", "", 4, scopeHolder},
	{"compose/scope.go", "comment", "owners", "", 1, scopeHolder},
	{"compose/scope_tracker.go", "identifier", "owner", "owner", 19, scopeHolder},
	{"compose/scope_tracker.go", "comment", "owner", "", 13, scopeHolder},
	{"compose/scope_tracker.go", "comment", "owners", "", 1, scopeHolder},

	{"node/peer_connections.go", "identifier", "owner", "owner", 5, gossipSlot},
	{"node/peer_connections.go", "identifier", "owner", "evictGossipOwnerLocked", 1, gossipSlot},
	{"node/peer_connections.go", "literal", "owner", `"%s: evicted gossip owner (grade=%s, reason=%s)"`, 1, gossipSlot},
	{"node/peer_connections.go", "comment", "owner", "", 1, gossipSlot},
	{"node/peer_connections.go", "identifier", "ownership", "dialOwnershipGrace", 2, dialClaim},
	{"node/peer_connections.go", "comment", "ownership", "", 3, dialClaim},
	{"node/mesh_connection.go", "identifier", "owner", "owner", 3, gossipSlot},
	{"node/mesh_connection.go", "identifier", "owner", "evictGossipOwnerLocked", 3, gossipSlot},
	{"node/mesh_connection.go", "comment", "owner", "", 1, gossipSlot},
	{"node/mesh_connection.go", "literal", "owner", `"[DISPATCH-TRACE] unregister exit: peer=%s decision=skipped-nil-not-owner currentProto=%s meshSessionsLen=%d"`, 1, meshSlot},
	{"node/mesh_connection.go", "literal", "owner", `"[DISPATCH-TRACE] unregister exit: peer=%s decision=skipped-not-owner expectedProto=%s currentProto=%s meshSessionsLen=%d"`, 1, meshSlot},
	{"node/runtime.go", "literal", "owner", `"mesh_unregister_skipped_not_owner"`, 1, meshSlot},

	{"directory/trust.go", "literal", "owner", `"trust: self-attestation is the owner-tombstone path, not observation"`, 1, recordAuth},
	{"node/lad_reach_bridge.go", "comment", "owner", "", 1, recordAuth},

	{"node/peer_connections.go", "comment", "organisation", "", 3, flyNetwork},
	{"node/peer_connections.go", "comment", "organisations", "", 3, flyNetwork},
	{"node/runtime.go", "comment", "organisations", "", 1, flyNetwork},
	{"node/peer_connections.go", "comment", "org", "", 1, flyNetwork},
	{"node/swarm_integration.go", "comment", "org", "", 1, flyNetwork},
	{"node/bidi_rpc.go", "comment", "organisation", "", 1, flyNetwork},
}

// applyExemptions splits findings into those no exemption excuses and a count
// of the findings each exemption excused.
func applyExemptions(findings []namingFinding, exemptions []namingExemption) ([]namingFinding, []int) {
	used := make([]int, len(exemptions))
	var open []namingFinding
	for _, f := range findings {
		if f.licence {
			continue
		}
		excused := false
		for i, e := range exemptions {
			if e.file == f.file && e.kind == f.kind && e.word == f.word && (e.token == "" || e.token == f.text) {
				used[i]++
				excused = true
				break
			}
		}
		if !excused {
			open = append(open, f)
		}
	}
	return open, used
}

// exemptionErrors reports each exemption with no reason or whose use differs
// from its pinned count.
func exemptionErrors(exemptions []namingExemption, used []int) []string {
	var out []string
	for i, e := range exemptions {
		if e.reason == "" {
			out = append(out, fmt.Sprintf("exemption %s %s %q %s has no reason", e.file, e.kind, e.word, e.token))
		}
		if used[i] != e.count {
			out = append(out, fmt.Sprintf("exemption %s %s %q %s is pinned at %d and matched %d: re-measure the site and restate the entry, or remove it when the word is gone",
				e.file, e.kind, e.word, e.token, e.count, used[i]))
		}
	}
	return out
}

type namingFinding struct {
	file    string
	line    int
	kind    string
	word    string
	text    string
	licence bool
}

type namingStats struct {
	goFiles    int
	generated  int
	protoFiles int
}

// scanNaming walks root and returns every finding. Directories whose name
// starts with a dot and directories holding their own go.mod (another module)
// are not walked.
func scanNaming(root string) ([]namingFinding, namingStats, error) {
	var out []namingFinding
	var stats namingStats
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(path)
		if d.IsDir() {
			if path == root {
				return nil
			}
			if strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir
			}
			out = append(out, stemFindings(rel, "directory name", d.Name())...)
			return nil
		}
		switch filepath.Ext(path) {
		case ".go":
			stats.goFiles++
		case ".proto":
			stats.protoFiles++
		default:
			out = append(out, stemFindings(rel, "file name", strings.TrimSuffix(d.Name(), filepath.Ext(path)))...)
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if isGenerated(rel, src) {
			stats.generated++
		}
		out = append(out, fileFindings(rel, src)...)
		return nil
	})
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].file != out[j].file {
			return out[i].file < out[j].file
		}
		return out[i].line < out[j].line
	})
	return out, stats, err
}

// isGenerated reports whether a .go file carries the generated-code marker
// before its package clause. A file that does not parse is read as
// hand-written, so a broken file is still scanned.
func isGenerated(file string, src []byte) bool {
	if filepath.Ext(file) != ".go" {
		return false
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.PackageClauseOnly|parser.ParseComments)
	return err == nil && ast.IsGenerated(parsed)
}

// fileFindings reads one .go or .proto file named file. go/scanner also
// tokenises .proto source: its identifiers, quoted strings and comments come
// out as the same token kinds, and its scan errors are ignored because only
// those tokens are read.
func fileFindings(file string, src []byte) []namingFinding {
	ext := filepath.Ext(file)
	out := stemFindings(file, "file name", strings.TrimSuffix(filepath.Base(file), ext))
	if isGenerated(file, src) {
		return out
	}
	fixtures := strings.HasSuffix(file, "_test.go")

	fset := token.NewFileSet()
	tf := fset.AddFile(file, -1, len(src))
	var s scanner.Scanner
	s.Init(tf, src, func(token.Position, string) {}, scanner.ScanComments)
	beforePackage := true
	for {
		pos, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		line := tf.Line(pos)
		switch tok {
		case token.PACKAGE:
			beforePackage = false
		case token.IDENT:
			out = append(out, wordFindings(file, "identifier", lit, line, false)...)
		case token.STRING, token.CHAR:
			if !fixtures {
				out = append(out, wordFindings(file, "literal", lit, line, false)...)
			}
		case token.COMMENT:
			out = append(out, wordFindings(file, "comment", lit, line, beforePackage && licenceHeader.MatchString(lit))...)
		}
	}
	return out
}

// stemFindings checks a file or directory name: a word that starts with a
// class stem, carries the product stem, or is a caller word or the product name,
// is a finding.
func stemFindings(file, kind, name string) []namingFinding {
	var out []namingFinding
	for _, w := range wordsOf(name) {
		folded := strings.ToLower(w.s)
		for _, stem := range namingStems {
			if strings.HasPrefix(folded, stem) {
				out = append(out, namingFinding{file: file, kind: kind, word: folded, text: name})
			}
		}
		if strings.Contains(folded, productStem) || callerWords[folded] || w.s == productName {
			out = append(out, namingFinding{file: file, kind: kind, word: folded, text: name})
		}
	}
	return out
}

// wordFindings checks one token for class words, the product stem, caller words
// and the product name, and a literal or a comment also for product hosts,
// reporting each on the line it sits on. licence marks the findings the licence
// header rule excuses.
func wordFindings(file, kind, text string, line int, licence bool) []namingFinding {
	var out []namingFinding
	at := func(off int) int { return line + strings.Count(text[:off], "\n") }
	for _, w := range wordsOf(text) {
		folded := strings.ToLower(w.s)
		barred := barredWords[folded] && !(folded == "org" && isTopLevelDomain(text, w))
		product := w.s == productName && !strings.HasSuffix(text[:w.off], moduleHost)
		if barred || product || callerWords[folded] || strings.Contains(folded, productStem) {
			out = append(out, namingFinding{file: file, line: at(w.off), kind: kind, word: folded, text: text, licence: licence})
		}
	}
	if kind == "identifier" {
		return out
	}
	for _, m := range productHost.FindAllStringIndex(text, -1) {
		out = append(out, namingFinding{file: file, line: at(m[0]), kind: kind,
			word: strings.ToLower(text[m[0]:m[1]]), text: text, licence: licence})
	}
	return out
}

// isTopLevelDomain reports whether w stands as the last label of a lower-case
// host name (golang.org), which is a DNS name and not a class word.
func isTopLevelDomain(text string, w cutWord) bool {
	if w.s != "org" || w.off < 2 || text[w.off-1] != '.' {
		return false
	}
	prev, _ := utf8.DecodeLastRuneInString(text[:w.off-1])
	if !(unicode.IsLower(prev) || unicode.IsDigit(prev) || prev == '-') {
		return false
	}
	end := w.off + len(w.s)
	if end == len(text) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(text[end:])
	return !(unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_')
}

type cutWord struct {
	s   string
	off int
}

// wordsOf cuts s into words: a word is a run of letters and digits, cut again
// where a lower-case letter or digit meets an upper-case one (crossOrigin),
// where an upper-case run meets a word (HTTPServer is HTTP, Server) and where
// letters meet digits.
func wordsOf(s string) []cutWord {
	var out []cutWord
	start := -1
	var prev rune
	flush := func(end int) {
		if start >= 0 && end > start {
			out = append(out, cutWord{s: s[start:end], off: start})
		}
		start = -1
	}
	for i, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush(i)
			prev = 0
			continue
		}
		if start < 0 {
			start = i
			prev = r
			continue
		}
		cut := false
		switch {
		case unicode.IsDigit(r) != unicode.IsDigit(prev):
			cut = true
		case unicode.IsUpper(r) && unicode.IsLower(prev):
			cut = true
		case unicode.IsUpper(r) && unicode.IsUpper(prev):
			next, _ := utf8.DecodeRuneInString(s[i+utf8.RuneLen(r):])
			cut = unicode.IsLower(next)
		}
		if cut {
			flush(i)
			start = i
		}
		prev = r
	}
	flush(len(s))
	return out
}

func clipText(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 120 {
		return s[:117] + "..."
	}
	return s
}
