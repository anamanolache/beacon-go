/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

// Package query implements a Beacon query on a BigQuery table.
package query

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
)

var basesRegex = regexp.MustCompile(`^([ACGT]+|N)$`)

type Query struct {
	ReferenceName  string
	ReferenceBases string
	AlternateBases string
	Start          *int64
	End            *int64
	StartMin       *int64
	StartMax       *int64
	EndMin         *int64
	EndMax         *int64
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
	if q.ReferenceName == "" {
		return errors.New("missing reference name")
	}
	if q.ReferenceBases == "" {
		return errors.New("missing reference bases")
	}
	if !basesRegex.MatchString(q.ReferenceBases) {
		return errors.New("invalid value for reference bases")
	}
	if q.AlternateBases == "" {
		return errors.New("missing alternate bases")
	}
	if !basesRegex.MatchString(q.AlternateBases) {
		return errors.New("invalid value for alternate bases")
	}
	if err := q.validateCoordinates(); err != nil {
		return fmt.Errorf("validating coordinates: %v", err)
	}
	return nil
}

func (q *Query) validateCoordinates() error {
	var precisePosition, imprecisePosition bool
	if q.Start != nil && (q.End != nil || q.ReferenceBases != "") {
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
	simpleClause("reference_name", q.ReferenceName)
	simpleClause("reference_bases", q.ReferenceBases)
	simpleClause("alternate_bases", q.AlternateBases)
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
