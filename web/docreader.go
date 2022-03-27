package web

import (
	"encoding/json"
	"io"

	"github.com/skillian/expr/errors"
)

type ResultsIterator struct {
	Fields []FieldDef
	Count  int64
	docs   chan *Document
	cancel func()
	done   chan error
}

func (it *ResultsIterator) Close() error {
	it.cancel()
	return <-it.done
}

func (it *ResultsIterator) ReadFrom(r io.Reader) (int64, error) {
	rc := &readCounter{r: r}
	dec := json.NewDecoder(rc)
	if err := readJSONOpenBrace(dec); err != nil {
		return rc.n, err
	}
	for dec.More() {
		s, err := readJSON[string](dec)
		if err != nil {
			return rc.n, err
		}
		switch s {
		case "Count":
			f, err := readJSON[float64](dec)
			if err != nil {
				return rc.n, err
			}
			it.Count = int64(f)
		case "Fields":
			if err := it.initFields(dec); err != nil {
				return rc.n, err
			}
		case "Docs":
			if err := it.readDocs(dec); err != nil {
				return rc.n, err
			}
		default:
			logger.Warn1(
				"unknown search results key: %q", s,
			)
		}
	}
	return rc.n, nil
}

func (it *ResultsIterator) readDocs(dec *json.Decoder) error {
	if err := readJSONOpenBracket(dec); err != nil {
		return err
	}
	for dec.More() {
		doc := &Document{}
		if err := dec.Decode(doc); err != nil {
			return errors.Errorf0From(
				err, "failed to decode a document",
			)
		}
		it.docs <- doc
	}
	return readJSONCloseBracket(dec)
}

func (it *ResultsIterator) initFields(dec *json.Decoder) error {
	if err := readJSONOpenBracket(dec); err != nil {
		return err
	}
	for dec.More() {
		it.Fields = append(it.Fields, FieldDef{})
		if err := dec.Decode(&it.Fields[len(it.Fields)-1]); err != nil {
			return errors.Errorf0From(
				err, "failed to decode a field",
			)
		}
	}
	return readJSONCloseBracket(dec)
}

func readJSONCloseBrace(dec *json.Decoder) error {
	return readJSONExpect(dec, json.Delim('}'))
}

func readJSONOpenBrace(dec *json.Decoder) error {
	return readJSONExpect(dec, json.Delim('{'))
}

func readJSONCloseBracket(dec *json.Decoder) error {
	return readJSONExpect(dec, json.Delim(']'))
}

func readJSONOpenBracket(dec *json.Decoder) error {
	return readJSONExpect(dec, json.Delim('['))
}

func readJSON[T any](dec *json.Decoder) (v T, err error) {
	t, err := readJSONToken(dec)
	if err != nil {
		return v, err
	}
	v, ok := t.(T)
	if !ok {
		return v, errors.Errorf2(
			"expected to read %[1]T, but "+
				"actually read %[2]v (type: %[2]T)",
			v, t,
		)
	}
	return
}

func readJSONToken(dec *json.Decoder) (any, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, errors.Errorf0From(
			err, "failed to read next token from JSON",
		)
	}
	return t, nil
}

func readJSONExpect(dec *json.Decoder, token any) error {
	t, err := readJSONToken(dec)
	if err != nil {
		return err
	}
	if t != token {
		return errors.Errorf2(
			"expected to read %[1]v (type: %[1]T), but "+
				"actually read %[2]v (type: %[2]T)",
			token, t,
		)
	}
	return nil
}
