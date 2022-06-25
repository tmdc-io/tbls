package datasource

import (
	"bytes"
	"encoding/json"
	"github.com/tmdc-io/tbls/drivers/oracle"
	"github.com/tmdc-io/tbls/utils"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tmdc-io/tbls/config"
	"github.com/tmdc-io/tbls/drivers"
	"github.com/tmdc-io/tbls/drivers/mariadb"
	"github.com/tmdc-io/tbls/drivers/mssql"
	"github.com/tmdc-io/tbls/drivers/mysql"
	"github.com/tmdc-io/tbls/drivers/postgres"
	"github.com/tmdc-io/tbls/drivers/redshift"
	"github.com/tmdc-io/tbls/drivers/snowflake"
	"github.com/tmdc-io/tbls/drivers/sqlite"
	"github.com/tmdc-io/tbls/schema"
	"github.com/xo/dburl"
)

// Analyze database
func Analyze(dsn config.DSN) (*schema.Schema, error) {
	//Set up logger
	utils.SetupLogging()

	urlstr := dsn.URL
	if strings.Index(urlstr, "https://") == 0 || strings.Index(urlstr, "http://") == 0 {
		return AnalyzeHTTPResource(dsn)
	}
	if strings.Index(urlstr, "json://") == 0 {
		return AnalyzeJSON(urlstr)
	}
	if strings.Index(urlstr, "bq://") == 0 || strings.Index(urlstr, "bigquery://") == 0 {
		return AnalyzeBigquery(urlstr)
	}
	if strings.Index(urlstr, "span://") == 0 || strings.Index(urlstr, "spanner://") == 0 {
		return AnalyzeSpanner(urlstr)
	}
	if strings.Index(urlstr, "dynamodb://") == 0 || strings.Index(urlstr, "dynamo://") == 0 {
		return AnalyzeDynamodb(urlstr)
	}
	s := &schema.Schema{}
	u, err := dburl.Parse(urlstr)
	if err != nil {
		return s, errors.WithStack(err)
	}
	splitted := strings.Split(u.Short(), "/")
	if len(splitted) < 2 {
		return s, errors.Errorf("invalid DSN: parse %s -> %#v", urlstr, u)
	}

	opts := []drivers.Option{}
	if u.Driver == "mysql" {
		values := u.Query()
		for k := range values {
			if k == "show_auto_increment" {
				opts = append(opts, mysql.ShowAutoIcrrement())
				values.Del(k)
			}
			if k == "hide_auto_increment" {
				opts = append(opts, mysql.HideAutoIcrrement())
				values.Del(k)
			}
		}
		u.RawQuery = values.Encode()
		urlstr = u.String()
	}
	db, err := dburl.Open(urlstr)
	defer db.Close()
	if err != nil {
		return s, errors.WithStack(err)
	}
	if err := db.Ping(); err != nil {
		return s, errors.WithStack(err)
	}

	var driver drivers.Driver

	switch u.Driver {
	case "postgres":
		s.Name = splitted[1]
		if u.Scheme == "rs" || u.Scheme == "redshift" {
			log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
			log.Info("Obtaining connection...")
			driver = redshift.New(db)
			log.Info("Connection established")
		} else {
			log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
			log.Info("Obtaining connection...")
			driver = postgres.New(db)
			log.Info("Connection established")
		}
	case "mysql":
		s.Name = splitted[1]
		if u.Scheme == "maria" || u.Scheme == "mariadb" {
			log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
			log.Info("Obtaining connection...")
			driver, err = mariadb.New(db, opts...)
			log.Info("Connection established")
		} else {
			log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
			log.Info("Obtaining connection...")
			driver, err = mysql.New(db, opts...)
			log.Info("Connection established")
		}
		if err != nil {
			return s, err
		}
	case "sqlite3":
		s.Name = splitted[len(splitted)-1]
		log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
		log.Info("Obtaining connection...")
		driver = sqlite.New(db)
		log.Info("Connection established")
	case "sqlserver":
		s.Name = splitted[1]
		log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
		log.Info("Obtaining connection...")
		driver = mssql.New(db, urlstr)
		log.Info("Connection established")
	case "snowflake":
		s.Name = splitted[2]
		log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
		log.Info("Obtaining connection...")
		driver = snowflake.New(db)
		log.Info("Connection established")
	case "oracle":
		s.Name = splitted[1]
		log.Infof("Driver : '%s' and Scheme '%s'",u.Driver, u.Scheme)
		log.Info("Obtaining connection...")
		driver = oracle.New(db, urlstr)
		log.Info("Connection established")
	default:
		return s, errors.Errorf("unsupported driver '%s'", u.Driver)
	}
	log.Info("Analyzing...")
	err = driver.Analyze(s)
	if err != nil {
		log.Errorf("Analyze: Failed analyizing : '%s' ",err.Error())
		return s, err
	}
	return s, nil
}

// AnalyzeHTTPResource analyze `https://` or `http://`
func AnalyzeHTTPResource(dsn config.DSN) (*schema.Schema, error) {
	s := &schema.Schema{}
	req, err := http.NewRequest("GET", dsn.URL, nil)
	if err != nil {
		log.Errorf("AnalyzeHTTPResource: Failed to make request : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	for k, v := range dsn.Headers {
		req.Header.Add(k, v)
	}
	client := &http.Client{Timeout: time.Duration(10) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Http error : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(s); err != nil {
		log.Errorf(":AnalyzeHTTPResource: Failed decoding : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	if err := s.Repair(); err != nil {
		log.Errorf("AnalyzeHTTPResource: Failed repairing : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	return s, nil
}

// AnalyzeJSON analyze `json://`
func AnalyzeJSON(urlstr string) (*schema.Schema, error) {
	s := &schema.Schema{}
	splitted := strings.Split(urlstr, "json://")
	file, err := os.Open(splitted[1])
	if err != nil {
		log.Errorf("AnalyzeJSON: Failed to open : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	dec := json.NewDecoder(file)
	if err := dec.Decode(s); err != nil {
		log.Errorf("AnalyzeJSON: Failed decoding : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	if err := s.Repair(); err != nil {
		log.Errorf("AnalyzeJSON: Failed repairing : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	return s, nil
}

// Deprecated
func AnalyzeJSONString(str string) (*schema.Schema, error) {
	return AnalyzeJSONStringOrFile(str)
}

// AnalyzeJSONStringOrFile analyze JSON string or JSON file
func AnalyzeJSONStringOrFile(strOrPath string) (s *schema.Schema, err error) {
	s = &schema.Schema{}
	var buf io.Reader
	if strings.HasPrefix(strOrPath, "{") {
		buf = bytes.NewBufferString(strOrPath)
	} else {
		buf, err = os.Open(filepath.Clean(strOrPath))
		if err != nil {
			return s, errors.WithStack(err)
		}
	}
	dec := json.NewDecoder(buf)
	if err := dec.Decode(s); err != nil {
		log.Errorf("AnalyzeJSONString: Failed decoding : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	if err := s.Repair(); err != nil {
		log.Errorf("AnalyzeJSONString: Failed reparing : '%s' ",err.Error())
		return s, errors.WithStack(err)
	}
	return s, nil
}
