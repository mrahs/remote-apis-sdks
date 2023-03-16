package blob

import (
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
)

// Blob is an immutable structure for raw bytes or a file path with the corresponding digest.
// The user is responsible for the relationship between the digest and the bytes or the file.
type Blob struct {
	digest    digest.Digest
	bytes     []byte
	path      ep.Abs
	predicate ep.Predicate
}

// Digest returns a copy of the blob's digest.
func (b *Blob) Digest() digest.Digest {
	return b.digest
}

// Bytes return a copy the blob's bytes.
func (b *Blob) Bytes() []byte {
	bytes := make([]byte, len(b.bytes))
	copy(bytes, b.bytes)
	return bytes
}

// BytesLen is a convenient method to probe for the bytes length without incurring the copy cost.
func (b *Blob) BytesLen() int {
	return len(b.bytes)
}

// Path returns the blob's path.
func (b *Blob) Path() ep.Abs {
	return b.path
}

// Predicate returns the predicate associated with this blob.
func (b *Blob) Predicate() ep.Predicate {
	return b.predicate
}

// FromBytes creates a new in-memory blob.
func FromBytes(digest digest.Digest, bytes []byte) Blob {
	return Blob{
		digest: digest,
		bytes:  bytes,
	}
}

// FromFile creates a new blob that references a file by its path.
func FromFile(digest digest.Digest, path ep.Abs) Blob {
	return Blob{
		digest: digest,
		path:   path,
	}
}

// FromDir creates a new blob that references a directory by its path with a predicate
// to exclude any descendent.
func FromDir(digest digest.Digest, path ep.Abs, predicate ep.Predicate) Blob {
	return Blob{
		digest:    digest,
		path:      path,
		predicate: predicate,
	}
}
