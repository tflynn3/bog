//go:build !linux

package listener

import "errors"

func owns(_, _ int) (bool, error) {
	return false, errors.New("candidate listener verification requires Linux")
}
