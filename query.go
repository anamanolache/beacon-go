package beacon

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
)

type Query struct {
	RefName  string
	RefBases string
	Start    *int64
	End      *int64
	StartMin *int64
	StartMax *int64
	EndMin   *int64
	EndMax   *int64
}

func (q *Query) Execute(ctx context.Context, projectID, tableID string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT count(v.reference_name) as count
		FROM %s as v
		WHERE %s
		LIMIT 1`,
		fmt.Sprintf("`%s`", tableID),
		q.whereClause(),
	)

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return false, fmt.Errorf("creating bigquery client: %v", err)
	}
	it, err := bqClient.Query(query).Read(ctx)
	if err != nil {
		return false, fmt.Errorf("querying database: %v", err)
	}

	var result struct {
		Count int
	}
	if err := it.Next(&result); err != nil {
		return false, fmt.Errorf("reading query result: %v", err)
	}
	return result.Count > 0, nil
}

func (q *Query) ValidateInput() error {
	if q.RefName == "" {
		return errors.New("missing chromosome name")
	}
	if q.RefBases == "" {
		return errors.New("missing referenceBases")
	}
	if err := q.validateCoordinates(); err != nil {
		return fmt.Errorf("validating coordinates: %v", err)
	}
	return nil
}

func (q *Query) validateCoordinates() error {
	var precisePosition, imprecisePosition bool
	if q.Start != nil && (q.End != nil || q.RefBases != "") {
		precisePosition = true
	}
	if q.StartMin != nil && q.StartMax != nil && q.EndMin != nil && q.EndMax != nil {
		imprecisePosition = true
	}

	if precisePosition && imprecisePosition {
		return errors.New("please query either precise or imprecise position")
	}
	if precisePosition || imprecisePosition {
		return nil
	}
	if q.Start != nil && q.End != nil || q.StartMin != nil || q.StartMax != nil || q.EndMin != nil || q.EndMax != nil {
		return errors.New("restrictions not met for provided coordinates")
	}
	return nil
}

func (q *Query) whereClause() string {
	var clauses []string
	add := func(clause string) {
		if clause != "" {
			clauses = append(clauses, clause)
		}
	}
	simpleClause := func(dbColumn, value string) {
		if dbColumn != "" && value != "" {
			add(fmt.Sprintf("%s='%s'", dbColumn, value))
		}
	}
	simpleClause("reference_name", q.RefName)
	simpleClause("reference_bases", q.RefBases)
	add(q.bqCoordinatesToWhereClause())
	return strings.Join(clauses, " AND ")
}

func (q *Query) bqCoordinatesToWhereClause() string {
	if q.Start != nil {
		if q.End != nil {
			return fmt.Sprintf("v.start = %d AND %d = v.end", *q.Start, *q.End)
		}
		return fmt.Sprintf("v.start = %d", *q.Start)
	}

	if q.StartMin != nil && q.StartMax != nil && q.EndMin != nil && q.EndMax != nil {
		return fmt.Sprintf("%d <= v.start AND v.start <= %d AND %d <= v.end AND v.end <= %d", *q.StartMin, *q.StartMax, *q.EndMin, *q.EndMax)
	}
	return ""
}
