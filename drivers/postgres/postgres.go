package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/aquasecurity/go-version/pkg/version"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tmdc-io/tbls/ddl"
	"github.com/tmdc-io/tbls/schema"
)

var reFK = regexp.MustCompile(`FOREIGN KEY \((.+)\) REFERENCES ([^\s]+)\s?\((.+)\)`)
var reVersion = regexp.MustCompile(`([0-9]+(\.[0-9]+)*)`)

// Postgres struct
type Postgres struct {
	db     *sql.DB
	rsMode bool
	currentSchema string
}

// New return new Postgres
func New(db *sql.DB, currentSchema string) *Postgres {
	return &Postgres{
		db:     db,
		rsMode: false,
		currentSchema: currentSchema,
	}
}

// Analyze PostgreSQL database schema
func (p *Postgres) Analyze(s *schema.Schema) error {
	d, err := p.Info()
	if err != nil {
		log.Errorf("Failed to query database info, err : '%s' ",err.Error())
		return errors.WithStack(err)
	}
	s.Driver = d

	// current schema
	var currentSchema string
	const CurrentSchemaQuery = "SELECT current_schema()";
	log.Infof("Running query to get current shcema : '%s'",CurrentSchemaQuery)
	schemaRows, err := p.db.Query(CurrentSchemaQuery)
	if err != nil {
		log.Errorf("Failed to query current shcema, err : '%s'\n\n ",err.Error())
		return errors.WithStack(err)
	}
	defer schemaRows.Close()
	for schemaRows.Next() {
		err := schemaRows.Scan(&currentSchema)
		if err != nil {
			log.Errorf("Failed to scan schemaRows, err : '%s'\n\n ",err.Error())
			return errors.WithStack(err)
		}
		log.Infof("Output : '%s'",currentSchema)
	}
	log.Infof("set current schema : '%s'\n\n",currentSchema)

	if p.currentSchema !="" {
		s.Driver.Meta.CurrentSchema = currentSchema
	}else {
		s.Driver.Meta.CurrentSchema = p.currentSchema
	}

	// search_path
	var searchPaths string
	const searchPathQuery = "SHOW search_path"
	log.Infof("Running query to get paths : '%s'",searchPathQuery)
	pathRows, err := p.db.Query(searchPathQuery)
	if err != nil {
		log.Errorf("Failed query to get search paths, err : '%s'\n\n ",err.Error())
		return errors.WithStack(err)
	}
	defer pathRows.Close()
	for pathRows.Next() {
		err := pathRows.Scan(&searchPaths)
		log.Infof("Output : '%s'",searchPaths)
		if err != nil {
			log.Errorf("Failed to scan pathRows, err : '%s\n\n' ",err.Error())
			return errors.WithStack(err)
		}
	}
	var paths = strings.Split(searchPaths, ", ");
	s.Driver.Meta.SearchPaths = paths
	log.Infof("Set searchPaths : '%s\n\n'",paths)


	fullTableNames := []string{}

	// tables
	var tablesDetailsQuery = "SELECT\n    cls.oid AS oid,\n    cls.relname AS table_name,\n    CASE\n        WHEN cls.relkind IN ('r', 'p') THEN 'BASE TABLE'\n        WHEN cls.relkind = 'v' THEN 'VIEW'\n        WHEN cls.relkind = 'm' THEN 'MATERIALIZED VIEW'\n        WHEN cls.relkind = 'f' THEN 'FOREIGN TABLE'\n    END AS table_type,\n    ns.nspname AS table_schema,\n    descr.description AS table_comment\nFROM pg_class AS cls\nINNER JOIN pg_namespace AS ns ON cls.relnamespace = ns.oid\nLEFT JOIN pg_description AS descr ON cls.oid = descr.objoid AND descr.objsubid = 0\nWHERE ns.nspname NOT IN ('pg_catalog', 'information_schema') \nAND cls.relkind IN ('r', 'p', 'v', 'f', 'm')\nORDER BY oid";
	if p.currentSchema !="" {
		tablesDetailsQuery = "SELECT\n    cls.oid AS oid,\n    cls.relname AS table_name,\n    CASE\n        WHEN cls.relkind IN ('r', 'p') THEN 'BASE TABLE'\n        WHEN cls.relkind = 'v' THEN 'VIEW'\n        WHEN cls.relkind = 'm' THEN 'MATERIALIZED VIEW'\n        WHEN cls.relkind = 'f' THEN 'FOREIGN TABLE'\n    END AS table_type,\n    ns.nspname AS table_schema,\n    descr.description AS table_comment\nFROM pg_class AS cls\nINNER JOIN pg_namespace AS ns ON cls.relnamespace = ns.oid\nLEFT JOIN pg_description AS descr ON cls.oid = descr.objoid AND descr.objsubid = 0\nWHERE ns.nspname NOT IN ('pg_catalog', 'information_schema') \nAND ns.nspname ='"+p.currentSchema+"' \nAND cls.relkind IN ('r', 'p', 'v', 'f', 'm')\nORDER BY oid";
	}
	log.Infof("Running query to get tables : '%s'",tablesDetailsQuery)
	tableRows, err := p.db.Query(tablesDetailsQuery)
	if err != nil {
		log.Errorf("Failed to query tables detail, err : '%s\n\n' ",err.Error())
		return errors.WithStack(err)
	}
	defer tableRows.Close()

	relations := []*schema.Relation{}

	tables := []*schema.Table{}
	for tableRows.Next() {
		var (
			tableOid     uint64
			tableName    string
			tableType    string
			tableSchema  string
			tableComment sql.NullString
		)

		err := tableRows.Scan(&tableOid, &tableName, &tableType, &tableSchema, &tableComment)

		if err != nil {
			log.Errorf("Failed to scan tableRows, err : '%s\n\n' ",err.Error())
			return errors.WithStack(err)
		}

		name := fmt.Sprintf("%s.%s", tableSchema, tableName)

		fullTableNames = append(fullTableNames, name)

		table := &schema.Table{
			Name:    name,
			Type:    tableType,
			Comment: tableComment.String,
		}
		table_json, _ := json.Marshal(table)
		log.Debugf("Table '%s' json:  %s\n\n", name,  table_json)

		// (materialized) view definition
		if tableType == "VIEW" || tableType == "MATERIALIZED VIEW" {
			log.Infof("Table type '%s\n", tableType)
			const viewDefinitionQuery = "SELECT pg_get_viewdef($1::oid);"
			log.Infof("Running query : '%s\n'",viewDefinitionQuery)
			viewDefRows, err := p.db.Query(viewDefinitionQuery, tableOid)
			if err != nil {
				log.Errorf("Failed to query view definition of '%s', err : '%s'\n\n ",name, err.Error())
				return errors.WithStack(err)
			}
			defer viewDefRows.Close()
			for viewDefRows.Next() {
				var tableDef sql.NullString
				err := viewDefRows.Scan(&tableDef)
				if err != nil {
					log.Errorf("Failed to scan viewDefRows, err : '%s\n\n' ",err.Error())
					return errors.WithStack(err)
				}
				log.Infof("Table definition : '%s'",tableDef.String)
				table.Def = fmt.Sprintf("CREATE %s %s AS (\n%s\n)", tableType, tableName, strings.TrimRight(tableDef.String, ";"))
				log.Debugf("Final table definition : '%s'\n\n",table.Def)
			}
		}

		// constraints
		log.Infof("Running query to get '%s' constraints : '%s'", name, p.queryForConstraints())
		constraintRows, err := p.db.Query(p.queryForConstraints(), tableOid)
		if err != nil {
			log.Errorf("Failed to query constraints of '%s', err : '%s'\n\n ",name, err.Error())
			return errors.WithStack(err)
		}
		defer constraintRows.Close()

		constraints := []*schema.Constraint{}

		for constraintRows.Next() {
			var (
				constraintName                  string
				constraintDef                   string
				constraintType                  string
				constraintReferencedTable       sql.NullString
				constraintColumnNames           []sql.NullString
				constraintReferencedColumnNames []sql.NullString
				constraintComment               sql.NullString
			)
			err = constraintRows.Scan(&constraintName, &constraintDef, &constraintType, &constraintReferencedTable, pq.Array(&constraintColumnNames), pq.Array(&constraintReferencedColumnNames), &constraintComment)
			if err != nil {
				log.Errorf("Failed to scan constraintRows, err : '%s'\n\n ", err.Error())
				return errors.WithStack(err)
			}
			rt := constraintReferencedTable.String
			constraint := &schema.Constraint{
				Name:              constraintName,
				Type:              convertConstraintType(constraintType),
				Def:               constraintDef,
				Table:             &table.Name,
				Columns:           arrayRemoveNull(constraintColumnNames),
				ReferencedTable:   &rt,
				ReferencedColumns: arrayRemoveNull(constraintReferencedColumnNames),
				Comment:           constraintComment.String,
			}
			constraintJson, _ := json.Marshal(constraint)
			log.Debugf("Constraing json : %s\n\n", constraintJson)

			if constraintType == "f" {
				relation := &schema.Relation{
					Table: table,
					Def:   constraintDef,
				}
				tableRelation, _ := json.Marshal(relation)
				log.Debugf("Table '%s' relation %s\n\n", name, tableRelation)

				relations = append(relations, relation)
			}
			constraints = append(constraints, constraint)
		}
		table.Constraints = constraints

		// triggers
		if !p.rsMode {
			const triggerQuery = `SELECT tgname, pg_get_triggerdef(trig.oid), descr.description AS comment
FROM pg_trigger AS trig
LEFT JOIN pg_description AS descr ON trig.oid = descr.objoid
WHERE tgisinternal = false
AND tgrelid = $1::oid
ORDER BY tgrelid`
			log.Infof("Running query to get '%s' triggers : '%s'", name, triggerQuery)
			triggerRows, err := p.db.Query(triggerQuery, tableOid)
			if err != nil {
				log.Errorf("Failed to query triggers of '%s', err : '%s'\n\n ",name, err.Error())
				return errors.WithStack(err)
			}
			defer triggerRows.Close()

			triggers := []*schema.Trigger{}
			for triggerRows.Next() {
				var (
					triggerName    string
					triggerDef     string
					triggerComment sql.NullString
				)
				err = triggerRows.Scan(&triggerName, &triggerDef, &triggerComment)
				if err != nil {
					log.Errorf("Failed to scan triggerRows of '%s', err : '%s'\n\n ",name, err.Error())
					return errors.WithStack(err)
				}
				trigger := &schema.Trigger{
					Name:    triggerName,
					Def:     triggerDef,
					Comment: triggerComment.String,
				}
				triggerJson, _ := json.Marshal(trigger)
				log.Debugf("Trigger: %s\n\n", triggerJson)

				triggers = append(triggers, trigger)
			}
			table.Triggers = triggers
		}

		// columns
		columnStmt, err := p.queryForColumns(s.Driver.DatabaseVersion)

		if err != nil {
			log.Errorf("Failed to create query for column statement., err : '%s'\n ", err.Error())
			return errors.WithStack(err)
		}
		log.Infof("Running query to get '%s' columns : '%s'", name, columnStmt)
		columnRows, err := p.db.Query(columnStmt, tableOid)
		if err != nil {
			log.Errorf("Failed to query columns of '%s', err : '%s'\n\n ",name, err.Error())
			return errors.WithStack(err)
		}
		defer columnRows.Close()

		columns := []*schema.Column{}
		for columnRows.Next() {
			var (
				columnName               string
				columnDefaultOrGenerated sql.NullString
				attrgenerated            sql.NullString
				isNullable               bool
				dataType                 string
				columnComment            sql.NullString
			)
			err = columnRows.Scan(&columnName, &columnDefaultOrGenerated, &attrgenerated, &isNullable, &dataType, &columnComment)
			if err != nil {
				log.Errorf("Failed to scan columnRows of '%s'\n\n, err : '%s' ",name, err.Error())
				return errors.WithStack(err)
			}
			column := &schema.Column{
				Name:     columnName,
				Type:     dataType,
				Nullable: isNullable,
				Comment:  columnComment.String,
			}

			switch attrgenerated.String {
			case "":
				column.Default = columnDefaultOrGenerated
			case "s":
				column.ExtraDef = fmt.Sprintf("GENERATED ALWAYS AS %s STORED", columnDefaultOrGenerated.String)
			default:
				return errors.Errorf("unsupported pg_attribute.attrgenerated '%s'", attrgenerated.String)
			}

			columnsJson, _ := json.Marshal(column)
			log.Debugf("%s\n\n", columnsJson)
			columns = append(columns, column)
		}
		table.Columns = columns

		// indexes
		log.Infof("Running query to get '%s' indexes : '%s'", name, p.queryForIndexes())
		indexRows, err := p.db.Query(p.queryForIndexes(), tableOid)
		if err != nil {
			log.Errorf("Failed to query indexes of '%s', err : '%s'\n\n ",name, err.Error())
			return errors.WithStack(err)
		}
		defer indexRows.Close()

		indexes := []*schema.Index{}
		for indexRows.Next() {
			var (
				indexName        string
				indexDef         string
				indexColumnNames []sql.NullString
				indexComment     sql.NullString
			)
			err = indexRows.Scan(&indexName, &indexDef, pq.Array(&indexColumnNames), &indexComment)
			if err != nil {
				log.Errorf("Failed to scan indexRows of '%s', err : '%s' \n",name, err.Error())
				return errors.WithStack(err)
			}
			index := &schema.Index{
				Name:    indexName,
				Def:     indexDef,
				Table:   &table.Name,
				Columns: arrayRemoveNull(indexColumnNames),
				Comment: indexComment.String,
			}
			indexJson, _ := json.Marshal(index)
			log.Debugf("%s\n\n", indexJson)
			indexes = append(indexes, index)
		}
		table.Indexes = indexes

		tables = append(tables, table)
		tablesJson, _ := json.Marshal(tables)

		log.Debugf("Final table '%s' struct", name)
		log.Debugf("%s\n\n\n\n", tablesJson)
	}
	s.Tables = tables
	log.Infof("Total '%d' tables scnaned.", len(tables))
	for _, t := range s.Tables{
		log.Infof("name:'%s',  type:'%s'", t.Name, t.Type)
	}

	// Relations
	for _, r := range relations {
		result := reFK.FindAllStringSubmatch(r.Def, -1)
		if len(result) < 1 || len(result[0]) < 4 {
			return errors.Errorf("can not parse foreign key: %s", r.Def)
		}
		strColumns := []string{}
		for _, c := range strings.Split(result[0][1], ", ") {
			strColumns = append(strColumns, strings.ReplaceAll(c, `"`, ""))
		}
		strParentTable := strings.ReplaceAll(result[0][2], `"`, "")
		strParentColumns := []string{}
		for _, c := range strings.Split(result[0][3], ", ") {
			strParentColumns = append(strParentColumns, strings.ReplaceAll(c, `"`, ""))
		}
		for _, c := range strColumns {
			column, err := r.Table.FindColumnByName(c)
			if err != nil {
				log.Errorf("err : '%s' ",err.Error())
				return err
			}
			r.Columns = append(r.Columns, column)
			column.ParentRelations = append(column.ParentRelations, r)
		}

		dn, err := detectFullTableName(strParentTable, s.Driver.Meta.SearchPaths, fullTableNames)
		if err != nil {
			log.Errorf("err : '%s' ",err.Error())
			return err
		}
		strParentTable = dn
		parentTable, err := s.FindTableByName(strParentTable)
		if err != nil {
			log.Errorf("err : '%s' ",err.Error())
			return err
		}
		r.ParentTable = parentTable
		for _, c := range strParentColumns {
			column, err := parentTable.FindColumnByName(c)
			if err != nil {
				log.Errorf("err : '%s' ",err.Error())
				return err
			}
			r.ParentColumns = append(r.ParentColumns, column)
			column.ChildRelations = append(column.ChildRelations, r)
		}
	}

	s.Relations = relations

	// referenced tables of view
	for _, t := range s.Tables {
		if t.Type != "VIEW" && t.Type != "MATERIALIZED VIEW" {
			continue
		}
		for _, rts := range ddl.ParseReferencedTables(t.Def) {
			rt, err := s.FindTableByName(rts)
			if err != nil {
				log.Errorf("err : '%s' ",err.Error())
				rt = &schema.Table{
					Name:     rts,
					External: true,
				}
			}
			t.ReferencedTables = append(t.ReferencedTables, rt)
		}
	}
	return nil
}

// Info return schema.Driver
func (p *Postgres) Info() (*schema.Driver, error) {
	var v string
	const selectVersionQuery = "SELECT version();"
	log.Infof("Running query : '%s'",selectVersionQuery)
	row := p.db.QueryRow(selectVersionQuery)
	err := row.Scan(&v)
	if err != nil {
		return nil, err
	}
	log.Infof("Output : '%s'\n\n",v)
	name := "postgres"
	if p.rsMode {
		name = "redshift"
	}

	d := &schema.Driver{
		Name:            name,
		DatabaseVersion: v,
		Meta:            &schema.DriverMeta{},
	}
	return d, nil
}

// EnableRsMode enable rsMode
func (p *Postgres) EnableRsMode() {
	p.rsMode = true
}

func (p *Postgres) queryForColumns(v string) (string, error) {
	verGeneratedColumn, err := version.Parse("12")
	if err != nil {
		return "", err
	}
	// v => PostgreSQL 9.5.24 on x86_64-pc-linux-gnu (Debian 9.5.24-1.pgdg90+1), compiled by gcc (Debian 6.3.0-18+deb9u1) 6.3.0 20170516, 64-bit
	matches := reVersion.FindStringSubmatch(v)
	if matches == nil || len(matches) < 2 {
		return "", errors.Errorf("malformed version: %s", v)
	}
	vv, err := version.Parse(matches[1])
	if err != nil {
		return "", err
	}
	if vv.LessThan(verGeneratedColumn) {
		return `
SELECT
    attr.attname AS column_name,
    pg_get_expr(def.adbin, def.adrelid) AS column_default,
    '' as dummy,
    NOT (attr.attnotnull OR tp.typtype = 'd' AND tp.typnotnull) AS is_nullable,
    CASE
        WHEN 'character varying'::regtype = ANY(ARRAY[attr.atttypid, tp.typelem]) THEN
            REPLACE(format_type(attr.atttypid, attr.atttypmod), 'character varying', 'varchar')
        ELSE format_type(attr.atttypid, attr.atttypmod)
    END AS data_type,
    descr.description AS comment
FROM pg_attribute AS attr
INNER JOIN pg_type AS tp ON attr.atttypid = tp.oid
LEFT JOIN pg_attrdef AS def ON attr.attrelid = def.adrelid AND attr.attnum = def.adnum AND attr.atthasdef
LEFT JOIN pg_description AS descr ON attr.attrelid = descr.objoid AND attr.attnum = descr.objsubid
WHERE
    attr.attnum > 0
AND NOT attr.attisdropped
AND attr.attrelid = $1::oid
ORDER BY attr.attnum;
`, nil
	} else {
		return `
SELECT
    attr.attname AS column_name,
    pg_get_expr(def.adbin, def.adrelid) AS column_default,
    attr.attgenerated,
    NOT (attr.attnotnull OR tp.typtype = 'd' AND tp.typnotnull) AS is_nullable,
    CASE
        WHEN 'character varying'::regtype = ANY(ARRAY[attr.atttypid, tp.typelem]) THEN
            REPLACE(format_type(attr.atttypid, attr.atttypmod), 'character varying', 'varchar')
        ELSE format_type(attr.atttypid, attr.atttypmod)
    END AS data_type,
    descr.description AS comment
FROM pg_attribute AS attr
INNER JOIN pg_type AS tp ON attr.atttypid = tp.oid
LEFT JOIN pg_attrdef AS def ON attr.attrelid = def.adrelid AND attr.attnum = def.adnum AND attr.atthasdef
LEFT JOIN pg_description AS descr ON attr.attrelid = descr.objoid AND attr.attnum = descr.objsubid
WHERE
    attr.attnum > 0
AND NOT attr.attisdropped
AND attr.attrelid = $1::oid
ORDER BY attr.attnum;
`, nil
	}
}

func (p *Postgres) queryForConstraints() string {
	if p.rsMode {
		return `
SELECT
  conname, pg_get_constraintdef(oid), contype, NULL, NULL, NULL, NULL
FROM pg_constraint
WHERE conrelid = $1::oid
ORDER BY conname`
	}
	return `
SELECT
  cons.conname AS name,
  CASE WHEN cons.contype = 't' THEN pg_get_triggerdef(trig.oid)
        ELSE pg_get_constraintdef(cons.oid)
  END AS def,
  cons.contype AS type,
  fcls.relname,
  ARRAY_AGG(attr.attname),
  ARRAY_AGG(fattr.attname),
  descr.description AS comment
FROM pg_constraint AS cons
LEFT JOIN pg_trigger AS trig ON trig.tgconstraint = cons.oid AND NOT trig.tgisinternal
LEFT JOIN pg_class AS fcls ON cons.confrelid = fcls.oid
LEFT JOIN pg_attribute AS attr ON attr.attrelid = cons.conrelid
LEFT JOIN pg_attribute AS fattr ON fattr.attrelid = cons.confrelid
LEFT JOIN pg_description AS descr ON cons.oid = descr.objoid
WHERE
	cons.conrelid = $1::oid
AND (cons.conkey IS NULL OR attr.attnum = ANY(cons.conkey))
AND (cons.confkey IS NULL OR fattr.attnum = ANY(cons.confkey))
GROUP BY cons.conindid, cons.conname, cons.contype, cons.oid, trig.oid, fcls.relname, descr.description
ORDER BY cons.conindid, cons.conname`
}

// arrayRemoveNull
func arrayRemoveNull(in []sql.NullString) []string {
	out := []string{}
	for _, i := range in {
		if i.Valid {
			out = append(out, i.String)
		}
	}
	return out
}

func (p *Postgres) queryForIndexes() string {
	if p.rsMode {
		return `
SELECT
  cls.relname AS indexname,
  pg_get_indexdef(idx.indexrelid) AS indexdef,
  NULL,
  NULL
FROM pg_index AS idx
INNER JOIN pg_class AS cls ON idx.indexrelid = cls.oid
WHERE idx.indrelid = $1::oid
ORDER BY idx.indexrelid`
	}
	return `
SELECT
  cls.relname AS indexname,
  pg_get_indexdef(idx.indexrelid) AS indexdef,
  ARRAY_AGG(attr.attname),
  descr.description AS comment
FROM pg_index AS idx
INNER JOIN pg_class AS cls ON idx.indexrelid = cls.oid
INNER JOIN pg_attribute AS attr ON idx.indexrelid = attr.attrelid
LEFT JOIN pg_description AS descr ON idx.indexrelid = descr.objoid
WHERE idx.indrelid = $1::oid
GROUP BY cls.relname, idx.indexrelid, descr.description
ORDER BY idx.indexrelid`
}

func detectFullTableName(name string, searchPaths, fullTableNames []string) (string, error) {
	if strings.Contains(name, ".") {
		return name, nil
	}
	fns := []string{}
	for _, n := range fullTableNames {
		if strings.HasSuffix(n, name) {
			for _, p := range searchPaths {
				// TODO: Support $user
				if n == fmt.Sprintf("%s.%s", p, name) {
					fns = append(fns, n)
				}
			}
		}
	}
	if len(fns) != 1 {
		return "", errors.Errorf("can not detect table name: %s", name)
	}
	return fns[0], nil
}

func convertConstraintType(t string) string {
	switch t {
	case "p":
		return "PRIMARY KEY"
	case "u":
		return "UNIQUE"
	case "f":
		return schema.TypeFK
	case "c":
		return "CHECK"
	case "t":
		return "TRIGGER"
	default:
		return t
	}
}
