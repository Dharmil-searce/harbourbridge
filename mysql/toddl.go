// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// schemaToDDL performs schema conversion from the source DB schema to
// Spanner. It uses the source schema in conv.SrcSchema, and writes
// the Spanner schema to conv.SpSchema.
func schemaToDDL(conv *internal.Conv) error {
	for _, srcTable := range conv.SrcSchema {
		spTableName, err := internal.GetSpannerTable(conv, srcTable.Name)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't map source table %s to Spanner: %s", srcTable.Name, err))
			continue
		}
		var spColNames []string
		spColDef := make(map[string]ddl.ColumnDef)
		conv.Issues[srcTable.Name] = make(map[string][]internal.SchemaIssue)
		// Iterate over columns using ColNames order.
		for _, srcColName := range srcTable.ColNames {
			srcCol := srcTable.ColDefs[srcColName]
			colName, err := internal.GetSpannerCol(conv, srcTable.Name, srcCol.Name, false)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Couldn't map source column %s of table %s to Spanner: %s", srcTable.Name, srcCol.Name, err))
				continue
			}
			spColNames = append(spColNames, colName)
			ty, issues := toSpannerType(conv, srcCol.Type.Name, srcCol.Type.Mods)
			if len(srcCol.Type.ArrayBounds) > 1 {
				ty = ddl.String{Len: ddl.MaxLength{}}
				issues = append(issues, internal.MultiDimensionalArray)
			}
			// TODO: add issues for all elements of srcCol.Ignored.
			if srcCol.Ignored.ForeignKey {
				issues = append(issues, internal.ForeignKey)
			}
			if srcCol.Ignored.Default {
				issues = append(issues, internal.DefaultValue)
			}
			if len(issues) > 0 {
				conv.Issues[srcTable.Name][srcCol.Name] = issues
			}
			spColDef[colName] = ddl.ColumnDef{
				Name:    colName,
				T:       ty,
				IsArray: len(srcCol.Type.ArrayBounds) == 1,
				NotNull: srcCol.NotNull,
				Comment: "From: " + quoteIfNeeded(srcCol.Name) + " " + printSourceType(srcCol.Type),
			}
		}
		comment := "Spanner schema for source table " + quoteIfNeeded(srcTable.Name)
		conv.SpSchema[spTableName] = ddl.CreateTable{
			Name:     spTableName,
			ColNames: spColNames,
			ColDefs:  spColDef,
			Pks:      cvtPrimaryKeys(conv, srcTable.Name, srcTable.PrimaryKeys),
			Comment:  comment}
	}
	return nil
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerType(conv *internal.Conv, id string, mods []int64) (ddl.ScalarType, []internal.SchemaIssue) {
	id1 := id
	var mysql_mods string
	if strings.Contains(id, "(") {
		id1 = strings.Split(id, "(")[0]
		mysql_mods = strings.Split(strings.Split(id, "(")[1], ")")[0]
	}
	if strings.Contains(id1, " ") {
		id1 = strings.Split(id, " ")[0]
	}
	switch id1 {
	case "bool", "boolean":
		return ddl.Bool{}, nil
	case "tinyint":
		if mysql_mods != "" {
			i, err := strconv.ParseInt(mysql_mods, 10, 64)
			if err != nil {
				panic(err)
			}
			if i == 1 {
				return ddl.Bool{}, nil
			} else {
				return ddl.Int64{}, []internal.SchemaIssue{internal.Widened}
			}
		}
		return ddl.Int64{}, []internal.SchemaIssue{internal.Widened}
	case "double":
		return ddl.Float64{}, nil
	case "float", "decimal":
		return ddl.Float64{}, []internal.SchemaIssue{internal.Widened}
	case "bigint":
		return ddl.Int64{}, nil
	case "smallint", "mediumint", "integer", "int":
		return ddl.Int64{}, []internal.SchemaIssue{internal.Widened}
	case "bit":
		return ddl.Bytes{Len: ddl.MaxLength{}}, nil
	case "varchar", "char":
		if mysql_mods != "" {
			i, err := strconv.ParseInt(mysql_mods, 10, 64)
			if err != nil {
				panic(err)
			}
			return ddl.String{Len: ddl.Int64Length{Value: i}}, nil
		}
		return ddl.String{Len: ddl.MaxLength{}}, nil
	case "text", "tinytext", "mediumtext", "longtext", "enum":
		return ddl.String{Len: ddl.MaxLength{}}, nil
	case "set":
		return ddl.String{Len: ddl.MaxLength{}}, nil
	case "json":
		return ddl.String{Len: ddl.MaxLength{}}, nil
	case "binary", "varbinary":
		return ddl.Bytes{Len: ddl.MaxLength{}}, nil
	case "tinyblob", "mediumblob", "blob", "longblob":
		return ddl.Bytes{Len: ddl.MaxLength{}}, nil
	case "date":
		return ddl.Date{}, nil
	case "timestamp", "datetime":
		return ddl.Timestamp{}, nil
	case "time", "year":
		return ddl.String{Len: ddl.MaxLength{}}, []internal.SchemaIssue{internal.Timestamp}
	}
	return ddl.String{Len: ddl.MaxLength{}}, []internal.SchemaIssue{internal.NoGoodType}
}

func printSourceType(ty schema.Type) string {
	s := ty.Name
	if len(ty.Mods) > 0 {
		var l []string
		for _, x := range ty.Mods {
			l = append(l, strconv.FormatInt(x, 10))
		}
		s = fmt.Sprintf("%s(%s)", s, strings.Join(l, ","))
	}
	if len(ty.ArrayBounds) > 0 {
		l := []string{s}
		for _, x := range ty.ArrayBounds {
			if x == -1 {
				l = append(l, "[]")
			} else {
				l = append(l, fmt.Sprintf("[%d]", x))
			}
		}
		s = strings.Join(l, "")
	}
	return s
}

func quoteIfNeeded(s string) string {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsPunct(r) {
			continue
		}
		return strconv.Quote(s)
	}
	return s
}

func cvtPrimaryKeys(conv *internal.Conv, srcTable string, srcKeys []schema.Key) []ddl.IndexKey {
	var spKeys []ddl.IndexKey
	for _, k := range srcKeys {
		spCol, err := internal.GetSpannerCol(conv, srcTable, k.Column, true)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't map key for table %s", srcTable))
			continue
		}
		spKeys = append(spKeys, ddl.IndexKey{Col: spCol, Desc: k.Desc})
	}
	return spKeys
}
