package oracle

import (
	"database/sql"
	"os"
	"testing"

	"github.com/k1LoW/tbls/schema"
	_ "github.com/sijms/go-ora/v2"
	"github.com/xo/dburl"
)

var s *schema.Schema
var db *sql.DB

func TestMain(m *testing.M) {
	s = &schema.Schema{
		Name: "SYSTEM",
	}
	db, _ = dburl.Open("or://system:Oradoc_db1@localhost:1521/ORCLCDB.LOCALDOMAIN")
	defer db.Close()
	exit := m.Run()
	if exit != 0 {
		os.Exit(exit)
	}
}

func TestAnalyzeView(t *testing.T) {
	driver := New(db)
	err := driver.Analyze(s)
	if err != nil {
		t.Errorf("%v", err)
	}
	view, _ := s.FindTableByName("SYSTEM.MVIEW_LOG")
	want := view.Type
	if want == "" {
		t.Errorf("got not empty string.")
	}
}

func TestExtraDef(t *testing.T) {
	driver := New(db)
	if err := driver.Analyze(s); err != nil {
		t.Fatal(err)
	}
	tbl, _ := s.FindTableByName("comments")
	{
		c, _ := tbl.FindColumnByName("post_id_desc")
		got := c.ExtraDef
		if want := "GENERATED ALWAYS AS (post_id * '-1'::integer) STORED"; got != want {
			t.Errorf("got %v\nwant %v", got, want)
		}
	}
}

func TestInfo(t *testing.T) {
	driver := New(db)
	d, err := driver.Info()
	if err != nil {
		t.Errorf("%v", err)
	}
	if d.Name != "oracle" {
		t.Errorf("got %v\nwant %v", d.Name, "postgres")
	}
	if d.DatabaseVersion == "" {
		t.Errorf("got not empty string.")
	}
}
