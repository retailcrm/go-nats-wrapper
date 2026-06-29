package natswrapper

import "errors"

var (
	ErrMessageRequired = errors.New("nats message is required")
	ErrSubjectRequired = errors.New("nats subject is required")
)
