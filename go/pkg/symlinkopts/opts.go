// Package symlinkopts provides an interface to create immutable and unambiguous symlink options with strong guarantees at compile time.
package symlinkopts

// Opts defines an immutable set of configuration for symlink handling.
type Opts struct {
	preserve        bool
	allowDangling   bool
	includeTarget   bool
	resolve         bool
	resolveExternal bool
}

// Preserve returns the corresponding property.
func (o *Opts) Preserve() bool {
	return o.preserve
}

// AllowDangling returns the corresponding property.
func (o *Opts) AllowDangling() bool {
	return o.allowDangling
}

// InlcudeTarget returns the corresponding property.
func (o *Opts) InlcudeTarget() bool {
	return o.includeTarget
}

// Resolve returns the corresponding property.
func (o *Opts) Resolve() bool {
	return o.resolve
}

// ResolveExternal returns the corresponding property.
func (o *Opts) ResolveExternal() bool {
	return o.resolveExternal
}

// ResolveOpts return the correct set of options to always resolve symlinks.
func ResolveOpts() Opts {
	return Opts{
		preserve:        false,
		allowDangling:   false,
		includeTarget:   true,
		resolve:         true,
		resolveExternal: true,
	}
}

// ResolveExternalOpts returns the correct set of options to only resolve symlinks
// if the target is outside the execution root. Otherwise, the symlink is preserved.
// This implies that all symlinks are followed, therefore, no dangling links are allowed.
// Additionally, this implies that targets are also included.
func ResolveExternalOpts() Opts {
	return Opts{
		preserve:        true,
		allowDangling:   false,
		includeTarget:   true,
		resolve:         false,
		resolveExternal: true,
	}
}

// PreserveWithTargetOpts returns the correct set of options to preserve all symlinks
// and include the targets.
// This implies that dangling links are not allowed.
func PreserveWithTargetOpts() Opts {
	return Opts{
		preserve:        true,
		allowDangling:   false,
		includeTarget:   true,
		resolve:         false,
		resolveExternal: false,
	}
}

// PreserveNoDanglingOpts returns the correct set of options to preserve all symlinks without targets.
// Targets need to be explicitly included.
// Dangling links are not allowed.
func PreserveNoDanglingOpts() Opts {
	return Opts{
		preserve:        true,
		allowDangling:   false,
		includeTarget:   false,
		resolve:         false,
		resolveExternal: false,
	}
}

// PreserveAllowDanglingOpts returns the correct set of options to preserve all symlinks without targets.
// Targets need to be explicitly included.
// Dangling links are allowed.
func PreserveAllowDanglingOpts() Opts {
	return Opts{
		preserve:        true,
		allowDangling:   true,
		includeTarget:   false,
		resolve:         false,
		resolveExternal: false,
	}
}
