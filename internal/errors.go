package internal

import (
	"fmt"
	"strings"
)

// Catch the error returned by f and store it into *p.  If *p is
// non-nil, a multi-error is written into it.
func Catch(p *error, f func() error) {
	*p = MultiError(*p, f())
}

type multiError []error

// ErrorfIfNotNil is a helper function that calls fmt.Errorf only if
// the given error is not nil.
func ErrorfIfNotNil(format string, args ...interface{}) error {
	_, ok := getErrorFromFormatArgs(format, args)
	if !ok {
		return nil
	}
	return fmt.Errorf(format, args...)
}

func getErrorFromFormatArgs(format string, args []interface{}) (error, bool) {
	i := 0
	formatting := false
	for _, r := range format {
		switch r {
		case '%':
			formatting = !formatting
		case 'w':
			if err, ok := args[i].(error); ok {
				return err, true
			}
			break
		default:
			if formatting {
				i++
				formatting = !formatting
			}
		}
	}
	return nil, false
}

// MultiError creates a single error value that wraps multiple errors.
func MultiError(errs ...error) error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	}
	delta := 0
	for i, err := range errs {
		if err == nil {
			delta--
			continue
		}
		errs[i+delta] = err
	}
	errs = errs[:len(errs)+delta]
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	}
	return multiError(errs)
}

func (errs multiError) Error() string {
	sb := strings.Builder{}
	for i, err := range errs {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString("• ")
		sb.WriteString(err.Error())
	}
	return sb.String()
}
