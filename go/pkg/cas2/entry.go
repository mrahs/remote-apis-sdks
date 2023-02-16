package cas2

import fp "github.com/bazelbuild/remote-apis-sdks/go/pkg/filepath"

type Entry struct {
  path fp.Abs
  filter PathFilter
}
