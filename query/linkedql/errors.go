package linkedql

import "fmt"

func formatMultiError(errors []error) error {
	joinedErr := ""
	for _, err := range errors {
		joinedErr += "; " + err.Error()
	}
	return fmt.Errorf("Could not parse PropertyPath: %v", joinedErr)
}
