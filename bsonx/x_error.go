package bsonx

import (
	"github.com/pkg/errors"
)

var errTooSmall = errors.New("eror: too small")

func IsTooSmall(err error) bool { return errors.Cause(err) == errTooSmall }

func NewErrTooSmall() error { return errors.WithStack(errTooSmall) }
