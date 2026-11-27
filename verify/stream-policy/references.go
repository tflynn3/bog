package streampolicy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

// IDs are opaque. Check exact tokens in text, including URLs and JSON
// strings, without interpreting a character within an ordinary word as an ID.
func containsReference(data []byte, id string) bool {
	if id == "" {
		return false
	}
	needle := []byte(id)
	for start := 0; start < len(data); {
		i := bytes.Index(data[start:], needle)
		if i < 0 {
			return false
		}
		i += start
		end := i + len(needle)
		left, _ := utf8.DecodeLastRune(data[:i])
		right, _ := utf8.DecodeRune(data[end:])
		if !referenceRune(left) && !referenceRune(right) {
			return true
		}
		start = i + 1
	}
	return false
}

func referenceRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) || r == '_' || r == '-'
}

func checkReferenceTokens(data []byte, ids []string) error {
	normalized := jsonUnescaped(data)
	surfaces := [][]byte{data}
	if !bytes.Equal(data, normalized) {
		surfaces = append(surfaces, normalized)
	}
	if bytes.ContainsRune(normalized, '%') {
		if decoded, err := url.PathUnescape(string(normalized)); err == nil {
			surfaces = append(surfaces, []byte(decoded))
		}
	}
	for _, surface := range surfaces {
		for _, id := range ids {
			if containsReference(surface, id) {
				return fmt.Errorf("%w: unexpected fixture marker in an opaque reference", ErrViolation)
			}
		}
	}
	return nil
}

func inspectDenialReferences(response Response, ids []string) error {
	if err := checkReferenceTokens(response.Body, ids); err != nil {
		return err
	}
	if hasDenialMetadata(response.Body) {
		return fmt.Errorf("%w: rejection disclosed structured rows, cursor, artifact, or retry metadata", ErrViolation)
	}
	for name, values := range response.Headers {
		if err := checkReferenceTokens([]byte(name), ids); err != nil {
			return err
		}
		field := strings.TrimPrefix(strings.ToLower(name), "x-")
		field = strings.TrimPrefix(field, "bog-")
		if isDeniedMetadataField(field) || field == "location" || field == "content-disposition" {
			return fmt.Errorf("%w: rejection disclosed artifact or retry metadata headers", ErrViolation)
		}
		for _, value := range values {
			if err := checkReferenceTokens([]byte(value), ids); err != nil {
				return err
			}
		}
	}
	return nil
}

func isDeniedMetadataField(field string) bool {
	field = strings.ToLower(field)
	field = strings.ReplaceAll(field, "-", "")
	field = strings.ReplaceAll(field, "_", "")
	switch field {
	case "artifact", "artifactid", "artifacturi", "artifacturl", "artifactlocation",
		"artifacts", "artifactsha256", "artifactchecksum", "artifactsize",
		"exportid", "downloadurl", "manifest", "artifactmanifest", "nextcursor",
		"retrykey", "retryoutcome", "requestfingerprint":
		return true
	default:
		return false
	}
}

// Check JSON object keys, including escaped or duplicate keys. The colon
// after a decoded string token identifies a key without matching prose values.
func hasDenialMetadata(body []byte) bool {
	if !json.Valid(body) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	for {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok {
			continue
		}
		rest := bytes.TrimLeft(body[decoder.InputOffset():], " \r\n\t")
		if len(rest) == 0 || rest[0] != ':' {
			continue
		}
		if isDeniedMetadataField(key) || isPopulatedRowCollection(key, rest[1:]) {
			return true
		}
	}
}

// A wire-level rows or records collection can expose data even if an adapter
// omitted it from its decoded result. Empty collections carry no rows.
func isPopulatedRowCollection(key string, value []byte) bool {
	if !strings.EqualFold(key, "rows") && !strings.EqualFold(key, "records") {
		return false
	}
	value = bytes.TrimLeft(value, " \r\n\t")
	switch value[0] {
	case '[':
		return bytes.TrimLeft(value[1:], " \r\n\t")[0] != ']'
	case 'n':
		return false // Only JSON null starts with n.
	default:
		return true
	}
}
