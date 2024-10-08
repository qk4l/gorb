package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateAcceptsAllowedServiceOptionsFlags(t *testing.T) {
	options := ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "dr", ShFlags: "sh-port|sh-fallback"}
	err := options.Validate(nil)

	assert.NoError(t, err)
}

func TestValidateRejectsInvalidServiceOptionsFlags(t *testing.T) {
	options := ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "dr", ShFlags: "sh-port|does-not-match"}
	err := options.Validate(nil)

	assert.EqualError(t, err, "specified flag is unknown")
}

func TestValidateAcceptsNoFlags(t *testing.T) {
	options := ServiceOptions{Port: 80, Host: "localhost", Protocol: "tcp", LbMethod: "dr"}
	err := options.Validate(nil)

	assert.NoError(t, err)
}
