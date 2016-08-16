package graph

import (
	"errors"
	"testing"
)

func TestIsQuadExist(t *testing.T) {
	tests := []struct {
		Err     error
		Matches bool
	}{
		{Err: nil, Matches: false},
		{Err: errors.New("foo"), Matches: false},
		{Err: ErrQuadExists, Matches: true},
		{Err: &DeltaError{Err: errors.New("foo")}, Matches: false},
		{Err: &DeltaError{Err: ErrQuadExists}, Matches: true},
	}

	for i, test := range tests {
		if match := IsQuadExist(test.Err); test.Matches != match {
			t.Errorf("%d> unexpected match: %t", i, match)
		}
	}
}

func TestIsQuadNotExist(t *testing.T) {
	tests := []struct {
		Err     error
		Matches bool
	}{
		{Err: nil, Matches: false},
		{Err: errors.New("foo"), Matches: false},
		{Err: ErrQuadNotExist, Matches: true},
		{Err: &DeltaError{Err: errors.New("foo")}, Matches: false},
		{Err: &DeltaError{Err: ErrQuadNotExist}, Matches: true},
	}

	for i, test := range tests {
		if match := IsQuadNotExist(test.Err); test.Matches != match {
			t.Errorf("%d> unexpected match: %t", i, match)
		}
	}
}

func TestIsInvalidAction(t *testing.T) {
	tests := []struct {
		Err     error
		Matches bool
	}{
		{Err: nil, Matches: false},
		{Err: errors.New("foo"), Matches: false},
		{Err: ErrInvalidAction, Matches: true},
		{Err: &DeltaError{Err: errors.New("foo")}, Matches: false},
		{Err: &DeltaError{Err: ErrInvalidAction}, Matches: true},
	}

	for i, test := range tests {
		if match := IsInvalidAction(test.Err); test.Matches != match {
			t.Errorf("%d> unexpected match: %t", i, match)
		}
	}
}
