package oracle

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"github.com/tmdc-io/tbls/schema"
	"regexp"
	"strings"
)

var reFK = regexp.MustCompile(`FOREIGN KEY \((.+)\) REFERENCES ([^\s]+)\s?\((.+)\)`)
var reVersion = regexp.MustCompile(`([0-9]+(\.[0-9]+)*)`)

// Oracle struct
type Oracle struct {
	db     *sql.DB
	schema string
	rsMode bool
}

// New return new Oracle
func New(db *sql.DB, url string) *Oracle {
	return &Oracle{
		db:     db,
		schema: extractSchema(url),
		rsMode: false,
	}
}

// extract schema from URL
func extractSchema(url string) (s string) {
	schm := strings.Split(url, "?current_schema=");
	if len(schm)==2{
		return schm[1]
	}
	return ""
}

// Analyze PostgreSQL database schema
func (p *Oracle) Analyze(s *schema.Schema) error {
	d, err := p.Info()
	if err != nil {
		return errors.WithStack(err)
	}
	s.Driver = d

	// current schema
	var currentSchema string

	// if schema provided then use it or else use current schema
	if len(p.schema) > 0{
		currentSchema = p.schema
		schemaRows, err := p.db.Query(fmt.Sprintf("alter session set current_schema = %s", currentSchema))
		if err != nil {
			return errors.WithStack(err)
		}
		defer schemaRows.Close()
	}else{
		schemaRows, err := p.db.Query(`select sys_context( 'userenv', 'current_schema' ) from dual`)
		if err != nil {
			return errors.WithStack(err)
		}
		defer schemaRows.Close()
		for schemaRows.Next() {
			err := schemaRows.Scan(&currentSchema)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	s.Driver.Meta.CurrentSchema = currentSchema

	// search_path
	//var searchPaths string
	//pathRows, err := p.db.Query(`SHOW search_path`)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//defer pathRows.Close()
	//for pathRows.Next() {
	//	err := pathRows.Scan(&searchPaths)
	//	if err != nil {
	//		return errors.WithStack(err)
	//	}
	//}
	s.Driver.Meta.SearchPaths = strings.Split(currentSchema, ", ")

	fullTableNames := []string{}

	// tables
	tableRows, err := p.db.Query(`select obj.object_id,
       obj.object_type,
       obj.owner,
       obj.object_name,
       def.text as definition,
       comm.comments
  from all_objects obj
       left outer join all_views def
           on obj.owner = def.owner
          and obj.object_name = def.view_name
       left outer join all_tab_comments comm
           on obj.object_name = comm.table_name
          and obj.owner = comm.owner
 where obj.object_type in ('TABLE','VIEW')
   and obj.owner = :1
 order by obj.owner, 
        obj.object_name`, currentSchema)

	if err != nil {
		return errors.WithStack(err)
	}
	defer tableRows.Close()

	relations := []*schema.Relation{}

	tables := []*schema.Table{}
	for tableRows.Next() {
		var (
			tableOid     uint64
			tableType    string
			tableSchema  string
			tableName    string
			definition   sql.NullString
			tableComment sql.NullString
		)
		err := tableRows.Scan(&tableOid, &tableType, &tableSchema, &tableName, &definition, &tableComment)
		if err != nil {
			return errors.WithStack(err)
		}
		name := fmt.Sprintf("%s.%s", tableSchema, tableName)

		fullTableNames = append(fullTableNames, name)

		table := &schema.Table{
			Name:    name,
			Type:    tableType,
			Def: 	 definition.String,
			Comment: tableComment.String,
		}

		// constraints
		constraintRows, err := p.db.Query(`with temp_table as  (
    SELECT a.constraint_name, 
        CASE c.constraint_type
            WHEN 'R'
                THEN 'FOREIGN KEY'
            WHEN 'P' 
                THEN 'PRIMARY KEY'
            WHEN 'U' 
                THEN 'UNIQUE KEY'
            ELSE c.constraint_type
        END constraint_type,
            c_pk.table_name r_table_name, 
            a.column_name,
            b.column_name r_column_name,
            '' as comments
      FROM all_cons_columns a
      LEFT JOIN all_constraints c ON a.owner = c.owner
                            AND a.constraint_name = c.constraint_name
      LEFT JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                               AND c.r_constraint_name = c_pk.constraint_name
      LEFT JOIN all_cons_columns b ON b.constraint_name=c_pk.constraint_name
     WHERE c.constraint_type in ('R','P','U')
       AND a.owner=:1 AND a.table_name = :2)   
	SELECT constraint_name, constraint_type, r_table_name,
        LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY constraint_name, constraint_type, r_table_name, comments) as column_name, 
        LISTAGG(r_column_name, ', ') WITHIN GROUP (ORDER BY constraint_name, constraint_type, r_table_name, comments) as r_column_name, comments
         FROM temp_table group by constraint_name, constraint_type, r_table_name, r_column_name, comments`, currentSchema, tableName)
		if err != nil {
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
				constraintColumnNames           string
				constraintReferencedColumnNames sql.NullString
				constraintComment               sql.NullString
			)
			err = constraintRows.Scan(&constraintName, &constraintType, &constraintReferencedTable, &constraintColumnNames, &constraintReferencedColumnNames, &constraintComment)
			if err != nil {
				return errors.WithStack(err)
			}
			switch constraintType {
			case "PRIMARY KEY":
				constraintDef = fmt.Sprintf("PRIMARY KEY (%s)", constraintColumnNames)
			case "UNIQUE":
				constraintDef = fmt.Sprintf("UNIQUE KEY %s (%s)", constraintName, constraintColumnNames)
			case "FOREIGN KEY":
				constraintType = schema.TypeFK
				constraintDef = fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)", constraintColumnNames, constraintReferencedTable.String, constraintReferencedColumnNames.String)
				relation := &schema.Relation{
					Table: table,
					Def:   constraintDef,
				}
				relations = append(relations, relation)
			case "UNKNOWN":
				constraintDef = fmt.Sprintf("UNKNOWN CONSTRAINT (%s) (%s) (%s)", constraintColumnNames, constraintReferencedTable.String, constraintReferencedColumnNames.String)
			}

			rt := constraintReferencedTable.String

			constraint := &schema.Constraint{
				Name:              constraintName,
				Type:              constraintType,
				Def:               constraintDef,
				Table:             &table.Name,
				ReferencedTable:   &rt,
				Columns:           strings.Split(constraintColumnNames, ", "),
				Comment:           constraintComment.String,
			}

			if constraintReferencedTable.String != "" {
				constraint.ReferencedTable = &constraintReferencedTable.String
				constraint.ReferencedColumns = strings.Split(constraintReferencedColumnNames.String, ", ")
			}
			constraints = append(constraints, constraint)
		}
		table.Constraints = constraints

		// triggers
		if !p.rsMode {
			triggerRows, err := p.db.Query(`SELECT trigger_name, trigger_body, description FROM USER_TRIGGERS where table_owner=:1 AND table_name=:2`, currentSchema, tableName)
			if err != nil {
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
					return errors.WithStack(err)
				}
				trigger := &schema.Trigger{
					Name:    triggerName,
					Def:     triggerDef,
					Comment: triggerComment.String,
				}
				triggers = append(triggers, trigger)
			}
			table.Triggers = triggers
		}

		// columns
		columnRows, err := p.db.Query(`select col.column_name,
       col.data_default,
       col.nullable,        
       col.data_type,
       comm.comments
  from all_tables tab
       inner join all_tab_columns col 
           on col.owner = tab.owner 
          and col.table_name = tab.table_name          
       left join all_col_comments comm
           on col.owner = comm.owner
          and col.table_name = comm.table_name 
          and col.column_name = comm.column_name 
 where col.owner =:1
 and tab.table_name =:2  
 order by col.owner,
       col.table_name, 
       col.column_name`, currentSchema, tableName)
		if err != nil {
			return errors.WithStack(err)
		}
		defer columnRows.Close()

		columns := []*schema.Column{}
		for columnRows.Next() {
			var (
				columnName               string
				columnDefaultOrGenerated sql.NullString
				isNullable               	 string
				dataType                 string
				columnComment            sql.NullString
			)
			err = columnRows.Scan(&columnName, &columnDefaultOrGenerated, &isNullable, &dataType, &columnComment)
			if err != nil {
				return errors.WithStack(err)
			}

			column := &schema.Column{
				Name:     columnName,
				Default:  columnDefaultOrGenerated,
				Type:     dataType,
				Comment:  columnComment.String,
			}
			column.Nullable=false
			if isNullable=="Y"{
				column.Nullable=true
			}
			columns = append(columns, column)
		}
		table.Columns = columns

		// indexes
		//indexRows, err := p.db.Query(p.queryForIndexes(), tableOid)
		//if err != nil {
		//	return errors.WithStack(err)
		//}
		//defer indexRows.Close()
		//
		//indexes := []*schema.Index{}
		//for indexRows.Next() {
		//	var (
		//		indexName        string
		//		indexDef         string
		//		indexColumnNames []sql.NullString
		//		indexComment     sql.NullString
		//	)
		//	err = indexRows.Scan(&indexName, &indexDef, pq.Array(&indexColumnNames), &indexComment)
		//	if err != nil {
		//		return errors.WithStack(err)
		//	}
		//	index := &schema.Index{
		//		Name:    indexName,
		//		Def:     indexDef,
		//		Table:   &table.Name,
		//		Columns: arrayRemoveNull(indexColumnNames),
		//		Comment: indexComment.String,
		//	}
		//
		//	indexes = append(indexes, index)
		//}
		//table.Indexes = indexes
		tables = append(tables, table)
	}

	s.Tables = tables

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
				return err
			}
			r.Columns = append(r.Columns, column)
			column.ParentRelations = append(column.ParentRelations, r)
		}

		dn, err := detectFullTableName(strParentTable, s.Driver.Meta.SearchPaths, fullTableNames)
		if err != nil {
			return err
		}
		strParentTable = dn
		parentTable, err := s.FindTableByName(strParentTable)
		if err != nil {
			return err
		}
		r.ParentTable = parentTable
		for _, c := range strParentColumns {
			column, err := parentTable.FindColumnByName(c)
			if err != nil {
				return err
			}
			r.ParentColumns = append(r.ParentColumns, column)
			column.ChildRelations = append(column.ChildRelations, r)
		}
	}

	s.Relations = relations

	return nil
}

// Info return schema.Driver
func (p *Oracle) Info() (*schema.Driver, error) {
	var v string
	row := p.db.QueryRow(`SELECT version FROM v$instance`)
	err := row.Scan(&v)
	if err != nil {
		return nil, err
	}

	name := "oracle"

	d := &schema.Driver{
		Name:            name,
		DatabaseVersion: v,
		Meta:            &schema.DriverMeta{},
	}
	return d, nil
}

// EnableRsMode enable rsMode
func (p *Oracle) EnableRsMode() {
	p.rsMode = true
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
