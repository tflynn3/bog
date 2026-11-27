package streampolicy

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// hasReference looks for the exact opaque value at token boundaries. An ID
// like "d" is a reference in /artifacts/d or "d", and remains part of an
// unrelated word in "denied".
func hasReference(surface, ref []byte) bool {
	if len(ref) == 0 {
		return false
	}
	for offset := 0; offset < len(surface); {
		at := bytes.Index(surface[offset:], ref)
		if at < 0 {
			return false
		}
		start := offset + at
		end := start + len(ref)
		before, n := utf8.DecodeLastRune(surface[:start])
		after, m := utf8.DecodeRune(surface[end:])
		if before == '.' {
			before, _ = utf8.DecodeLastRune(surface[:start-n])
		}
		if after == '.' {
			after, _ = utf8.DecodeRune(surface[end+m:])
		}
		if !referenceRune(before) && !referenceRune(after) {
			return true
		}
		offset = start + 1
	}
	return false
}

func referenceRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) ||
		r == '_' || r == '-' || r == '~'
}

// uriUnescaped inspects percent-encoded IDs in links and Location headers.
// Preserve literal percent characters and continue through surrounding prose.
func uriUnescaped(surface []byte) []byte {
	start := bytes.IndexByte(surface, '%')
	if start < 0 {
		return surface
	}
	decoded := make([]byte, 0, len(surface))
	decoded = append(decoded, surface[:start]...)
	for i := start; i < len(surface); i++ {
		if surface[i] == '%' && i+2 < len(surface) {
			if value, err := strconv.ParseUint(string(surface[i+1:i+3]), 16, 8); err == nil {
				decoded = append(decoded, byte(value))
				i += 2
				continue
			}
		}
		decoded = append(decoded, surface[i])
	}
	return decoded
}

func inspectReferences(response Response, references []string) error {
	if len(references) == 0 {
		return nil
	}
	return inspectSurfaces(response, func(surface []byte) error {
		json := jsonUnescaped(surface)
		decoded := uriUnescaped(json)
		for _, reference := range references {
			ref := []byte(reference)
			// Scan the original spelling only when an ID itself includes an
			// escape character. Otherwise normalization supplies the real
			// boundaries, including in an escaped word such as d\u0065nied.
			if hasReference(decoded, ref) ||
				(bytes.ContainsRune(ref, '%') && hasReference(json, ref)) ||
				(bytes.ContainsRune(ref, '\\') && hasReference(surface, ref)) {
				return fmt.Errorf("%w: unexpected fixture marker at an opaque reference boundary", ErrViolation)
			}
		}
		return nil
	})
}
