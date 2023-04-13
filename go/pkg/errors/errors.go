// Package errors provides the functionality of wrapping multiple errors from go1.20.
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

func Is(err, target error) bool {
	je, ok := err.(*joinError)
	if !ok {
		return errors.Is(err, target)
	}

	for _, e := range je.errs {
		if errors.Is(e, target) {
			return true
		}
	}
	return false
}

func New(text string) error {
	return errors.New(text)
}
