// Package errors provides the ability to wrap multiple errors while maintaining API compatibility with the standard package.
package errors

import "errors"

type joinError struct {
	errs []error
}

func (e *joinError) Error() string {
	var b []byte
	for i, err := range e.errs {
		if i > 0 {
			b = append(b, '\n')
		}
		b = append(b, err.Error()...)
	}
	return string(b)
}

// Join wraps the specified errors into one error that prints the entire list using newline as the delimiter.
func Join(errs ...error) error {
	n := 0
	for _, err := range errs {
		if err != nil {
			n++
		}
	}

	if n == 0 {
		return nil
	}

	// Unlike go1.20, this allows for efficient and convenient wrapping of errors without additional guards.
	// E.g. the following code returns err as is without any allocations if errClose is nil:
	// errClose := f.Close(); err = errors.Join(errClose, err)
	if n == 1 {
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}

	e := &joinError{
		errs: make([]error, 0, n),
	}
	for _, err := range errs {
		if err != nil {
			e.errs = append(e.errs, err)
		}
	}
	return e
}

// Is implements the corresponding interface allowing the joinError type to be forwards compatible.
func (e *joinError) Is(target error) bool {
	for _, e := range e.errs {
		if errors.Is(e, target) {
			return true
		}
	}
	return false
}

// Is delegates to the standard package.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// New delegates to the standard package.
func New(text string) error {
	return errors.New(text)
}
