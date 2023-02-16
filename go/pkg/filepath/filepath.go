package filepath

import (
	"fmt"
	"path/filepath"
)

type Path struct {
  path string
}

type Meta struct {

}

type Filter func(path string) bool

func NewAbs(path string) (Path, error) {
  if filepath.IsAbs(path) {
    return Path{filepath.Clean(path)}, nil
  }
  return Path{}, fmt.Errorf("path is not absolute: %q", path)
}

func NewRel(path string) (Rel, error) {
  if filepath.IsAbs(path) {
    return Rel{}, fmt.Errorf("path is not relative: %q", path)
  }
  return Rel{filepath.Clean(path)}, nil
}
