package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// Check the original tokens before decoding into structs or maps, where a
// later value could overwrite earlier evidence. Go's struct decoder also
// accepts alternate casing, so reject case-folded repeats.
func rejectDuplicateReportFields(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return checkReportValue(dec)
}

func checkReportValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]struct{})
		for dec.More() {
			token, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return fmt.Errorf("expected report field name at byte %d", dec.InputOffset())
			}
			folded := strings.Map(foldReportRune, key)
			if _, exists := seen[folded]; exists {
				return fmt.Errorf("duplicate report field %q at byte %d", key, dec.InputOffset())
			}
			seen[folded] = struct{}{}
			if err := checkReportValue(dec); err != nil {
				return err
			}
		}
	case json.Delim('['):
		for dec.More() {
			if err := checkReportValue(dec); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	_, err = dec.Token()
	return err
}

// SimpleFold visits equivalent runes in order, then wraps to the smallest.
func foldReportRune(r rune) rune {
	for {
		next := unicode.SimpleFold(r)
		if next <= r {
			return next
		}
		r = next
	}
}
