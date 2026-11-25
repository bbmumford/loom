/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package node

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
)

// RoleRelease gates the release of a database group's key on this node's role
// endorsement (directory.RoleEndorsement). A group GroupRoles names has its key
// asked of DatabasesConfig.KeyResolver only once this node has signed, under a
// role key it holds, an endorsement of its own ID and identity key for that
// role, and the endorsement verifies under Keys. The node re-signs every
// republish period; when a group's endorsement no longer verifies and cannot be
// renewed, Lapsed is called, because the group's key is already in this process
// and only a stopped process no longer holds it.
type RoleRelease struct {
	// Keys is what the signed mesh document says about the guarded roles.
	Keys directory.RoleKeys
	// Held is the private key of each role this node's own document names, by
	// role.
	Held map[string]ed25519.PrivateKey
	// GroupRoles maps a group to the role whose endorsement releases its key.
	// A group it does not name is released as DatabasesConfig states.
	GroupRoles map[string]string
	// SecretRoles maps a named secret the host holds outside any group (an
	// organisation token, a group's token) to the role whose endorsement
	// releases it. Each is endorsed when Initialize opens the groups and
	// renewed with them; the host reads the secret only once SecretReleased
	// answers nil for its name.
	SecretRoles map[string]string
	// Lapsed is told the group or secret name and the role whose endorsement
	// lapsed, and why. It is required; the endpoint spine's stops the process.
	Lapsed func(name, role string, err error)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

func (r *RoleRelease) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// validate checks what the release needs before Initialize opens anything.
func (r *RoleRelease) validate() error {
	if r == nil {
		return nil
	}
	if r.Lapsed == nil {
		return errors.New("databases role release has no Lapsed handler")
	}
	if r.Keys.RepublishPeriod <= 0 {
		return errors.New("databases role release has no republish period")
	}
	for group, role := range r.GroupRoles {
		if role == "" {
			return fmt.Errorf("databases group %q names an empty role", group)
		}
	}
	for name, role := range r.SecretRoles {
		if role == "" {
			return fmt.Errorf("databases secret %q names an empty role", name)
		}
	}
	return nil
}

// roleEndorsements holds this node's current endorsement of each role a held
// group needs.
type roleEndorsements struct {
	mu  sync.Mutex
	set map[string]directory.RoleEndorsement
}

// endorse signs and verifies this node's endorsement for role, or returns why
// it cannot.
func (r *RoleRelease) endorse(role string, id ports.NodeID, nodeKey ed25519.PublicKey) (directory.RoleEndorsement, error) {
	priv, ok := r.Held[role]
	if !ok {
		return directory.RoleEndorsement{}, fmt.Errorf("this node holds no key for role %q", role)
	}
	now := r.now()
	e, err := directory.SignRoleEndorsement(priv, role, id, nodeKey, now, r.Keys)
	if err != nil {
		return directory.RoleEndorsement{}, err
	}
	if err := directory.VerifyRoleEndorsement(e, r.Keys, now); err != nil {
		return directory.RoleEndorsement{}, err
	}
	return e, nil
}

// release returns the endorsement, by role, of every role the groups need,
// refusing the first group whose role this node cannot endorse.
func (r *RoleRelease) release(groups []string, id ports.NodeID, nodeKey ed25519.PublicKey) (map[string]directory.RoleEndorsement, error) {
	out := map[string]directory.RoleEndorsement{}
	if r == nil {
		return out, nil
	}
	for _, g := range groups {
		role, ok := r.GroupRoles[g]
		if !ok {
			continue
		}
		if _, done := out[role]; done {
			continue
		}
		e, err := r.endorse(role, id, nodeKey)
		if err != nil {
			return nil, fmt.Errorf("databases group %q: role %q: no valid endorsement, so its key is not released: %w", g, role, err)
		}
		out[role] = e
	}
	names := make([]string, 0, len(r.SecretRoles))
	for name := range r.SecretRoles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		role := r.SecretRoles[name]
		if _, done := out[role]; done {
			continue
		}
		e, err := r.endorse(role, id, nodeKey)
		if err != nil {
			return nil, fmt.Errorf("databases secret %q: role %q: no valid endorsement, so it is not released: %w", name, role, err)
		}
		out[role] = e
	}
	return out, nil
}

// renew re-signs each endorsement in held once per republish period until ctx
// ends. A renewal that fails while the current endorsement still verifies is
// tried again next period; once the current one no longer verifies (it expired,
// or its role key left Keys), Lapsed is told for every group that role
// releases.
func (r *RoleRelease) renew(ctx context.Context, held *roleEndorsements, id ports.NodeID, nodeKey ed25519.PublicKey) {
	t := time.NewTicker(r.Keys.RepublishPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.renewOnce(held, id, nodeKey)
	}
}

func (r *RoleRelease) renewOnce(held *roleEndorsements, id ports.NodeID, nodeKey ed25519.PublicKey) {
	held.mu.Lock()
	defer held.mu.Unlock()
	for role, current := range held.set {
		next, err := r.endorse(role, id, nodeKey)
		if err == nil {
			held.set[role] = next
			continue
		}
		if verr := directory.VerifyRoleEndorsement(current, r.Keys, r.now()); verr == nil {
			continue
		}
		for group, gr := range r.GroupRoles {
			if gr == role {
				r.Lapsed(group, role, err)
			}
		}
		for name, sr := range r.SecretRoles {
			if sr == role {
				r.Lapsed(name, role, err)
			}
		}
	}
}

// ErrSecretNotReleased refuses a named secret whose role this node holds no
// current endorsement of, or that no RoleRelease.SecretRoles entry names.
var ErrSecretNotReleased = errors.New("databases secret is not released")

// SecretReleased answers nil when name is a RoleRelease.SecretRoles entry and
// this node's current endorsement of its role verifies now under the
// release's RoleKeys; otherwise ErrSecretNotReleased with the reason. The
// host reads the named secret from its vault only after a nil answer.
func (rt *Runtime) SecretReleased(name string) error {
	if rt == nil || rt.cfg.Databases == nil || rt.cfg.Databases.Roles == nil {
		return fmt.Errorf("%w: %q: no role release is configured", ErrSecretNotReleased, name)
	}
	r := rt.cfg.Databases.Roles
	role, ok := r.SecretRoles[name]
	if !ok {
		return fmt.Errorf("%w: %q: no role releases it", ErrSecretNotReleased, name)
	}
	e, ok := rt.RoleEndorsement(role)
	if !ok {
		return fmt.Errorf("%w: %q: no endorsement of role %q", ErrSecretNotReleased, name, role)
	}
	if err := directory.VerifyRoleEndorsement(e, r.Keys, r.now()); err != nil {
		return fmt.Errorf("%w: %q: role %q: %v", ErrSecretNotReleased, name, role, err)
	}
	return nil
}

// roleEndorsementTags returns the RoleEndorsementTag entries PeerPublisher
// writes on every publish: one per role this node holds a current
// endorsement of, in role order.
func (rt *Runtime) roleEndorsementTags() []string {
	if rt == nil || rt.roleEndorsements == nil {
		return nil
	}
	rt.roleEndorsements.mu.Lock()
	defer rt.roleEndorsements.mu.Unlock()
	roles := make([]string, 0, len(rt.roleEndorsements.set))
	for role := range rt.roleEndorsements.set {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	out := make([]string, 0, len(roles))
	for _, role := range roles {
		out = append(out, directory.RoleEndorsementTag(role)+"="+rt.roleEndorsements.set[role].Encode())
	}
	return out
}

// RoleEndorsement returns this node's current endorsement of role, when a held
// group or secret needs that role.
func (rt *Runtime) RoleEndorsement(role string) (directory.RoleEndorsement, bool) {
	if rt == nil || rt.roleEndorsements == nil {
		return directory.RoleEndorsement{}, false
	}
	rt.roleEndorsements.mu.Lock()
	defer rt.roleEndorsements.mu.Unlock()
	e, ok := rt.roleEndorsements.set[role]
	return e, ok
}
