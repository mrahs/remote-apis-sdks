// Package symlinkopts provides an efficient interface to create unambiguous symlink options.
package symlinkopts

import (
	"fmt"
	"strings"
)

// Options represents a set of options for handling symlinks.
// To ensure a valid set of options, use one of the functions provided in this package
// to get the desired options.
type Options uint32

const (
	Preserve Options = 1 << (32 - 1 - iota)
	NoDangling
	IncludeTarget
	Resolve
	ResolveExternal
)

// String returns a string representation of the options.
//
// The string has at most five letters:
//
//	P for Preserve
//	N for No Dangling
//	I for Include Target
//	R for Replace
//	E for Replace External
func (o Options) String() string {
	var b strings.Builder
	if o.Preserve() {
		fmt.Fprint(&b, "P")
	}
	if o.NoDangling() {
		fmt.Fprint(&b, "N")
	}
	if o.IncludeTarget() {
		fmt.Fprint(&b, "I")
	}
	if o.Resolve() {
		fmt.Fprint(&b, "R")
	}
	if o.ResolveExternal() {
		fmt.Fprint(&b, "E")
	}
	return b.String()
}

// Preserve returns true if the options include the corresponding property.
func (o Options) Preserve() bool {
	return o&Preserve == Preserve
}

// NoDangling returns true if the options includes the corresponding property.
func (o Options) NoDangling() bool {
	return o&NoDangling == NoDangling
}

// IncludeTarget returns true if the options includes the corresponding property.
func (o Options) IncludeTarget() bool {
	return o&IncludeTarget == IncludeTarget
}

// Resolve returns true if the options includes the corresponding property.
func (o Options) Resolve() bool {
	return o&Resolve == Resolve
}

// ResolveExternal returns true if the options includes the corresponding property.
func (o Options) ResolveExternal() bool {
	return o&ResolveExternal == ResolveExternal
}

// ResolveAlways return the correct set of options to always resolve symlinks.
// This implies that symlinks are followed and no dangling symlinks are allowed.
// Each target will have the path of the symlink.
func ResolveAlways() Options {
	return NoDangling | IncludeTarget | Resolve | ResolveExternal
}

// ResolveExternalOnly returns the correct set of options to only resolve symlinks
// if the target is outside the execution root. Otherwise, the symlink is preserved.
// This implies that all symlinks are followed, therefore, no dangling links are allowed.
// Otherwise, it's not possible to guarantee that all required files are under the execution root.
// Targets of non-external symlinks are not included.
func ResolveExternalOnly() Options {
	return Preserve | NoDangling | ResolveExternal
}

// ResolveExternalOnlyWithTarget is like ResolveExternalOnly but targets of non-external symlinks are included.
func ResolveExternalOnlyWithTarget() Options {
	return Preserve | NoDangling | IncludeTarget | ResolveExternal
}

// PreserveWithTarget returns the correct set of options to preserve all symlinks
// and include the targets.
// This implies that dangling links are not allowed.
func PreserveWithTarget() Options {
	return Preserve | NoDangling | IncludeTarget
}

// PreserveNoDangling returns the correct set of options to preserve all symlinks without targets.
// Targets need to be explicitly included.
// Dangling links are not allowed.
func PreserveNoDangling() Options {
	return Preserve | NoDangling
}

// PreserveAllowDangling returns the correct set of options to preserve all symlinks without targets.
// Targets need to be explicitly included.
// Dangling links are allowed.
func PreserveAllowDangling() Options {
	return Preserve
}
