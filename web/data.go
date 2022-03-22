package web

import "github.com/skillian/errors"

// SearchCriterion is a single search parameter
type SearchCriterion struct {
	// Prompt ID or text for the search criterion.
	Prompt IDAndNamerFilter

	// Value to use for the search.
	Value interface{}
}

// dbs holds metadata about all the databases accessible to a session.
type dbs struct {
	elems []db
	lookup
}

func (dbs *dbs) db(id ID) (*db, error) {
	i, ok := dbs.byID[id]
	if !ok {
		return nil, errors.Errorf(
			"failed to get database from cache by ID: %d", id)
	}
	return &dbs.elems[i], nil
}

func (dbs *dbs) arch(dbid, arid ID) (*arch, error) {
	db, err := dbs.db(dbid)
	if err != nil {
		return nil, err
	}
	i, ok := db.archs.byID[arid]
	if !ok {
		return nil, errors.Errorf(
			"failed to get archive from cache by ID: %d", arid)
	}
	return &db.archs.elems[i], nil
}

func (dbs *dbs) search(dbid, arid, srid ID) (*search, error) {
	ar, err := dbs.arch(dbid, arid)
	if err != nil {
		return nil, err
	}
	i, ok := ar.searches.byID[srid]
	if !ok {
		return nil, errors.Errorf(
			"failed to get search from cache by ID: %d", srid)
	}
	return &ar.searches.elems[i], nil
}

// db is a per-session cache of a Square 9 database
type db struct {
	Database
	archs
}

type archs struct {
	elems []arch
	lookup
}

func makeArchs(as []Archive) archs {
	ars := archs{elems: make([]arch, len(as))}
	for i, a := range as {
		ars.elems[i].Archive = a
	}
	ars.lookup = lookupOf(ars.elems)
	return ars
}

// arch is a per-sesion cache of a Square 9 archive
type arch struct {
	Archive
	searches
	fields
}

type searches struct {
	elems []search
	lookup
}

type search struct {
	Search
	lookup
}

type fields struct {
	elems []FieldDef
	lookup
}
