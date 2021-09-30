module github.com/tmdc-io/tbls

go 1.16

require (
	cloud.google.com/go/bigquery v1.18.0
	cloud.google.com/go/spanner v1.18.0
	github.com/antonmedv/expr v1.8.9
	github.com/aquasecurity/go-version v0.0.0-20210121072130-637058cfe492
	github.com/aws/aws-sdk-go v1.38.39
	github.com/beta/freetype v0.0.1
	github.com/denisenkom/go-mssqldb v0.10.0
	github.com/gertd/go-pluralize v0.1.7
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gobuffalo/logger v1.0.4 // indirect
	github.com/gobuffalo/packr/v2 v2.8.1
	github.com/goccy/go-graphviz v0.0.9
	github.com/goccy/go-yaml v1.8.9
	github.com/google/go-cmp v0.5.5
	github.com/k1LoW/ffff v0.2.0
	github.com/karrick/godirwalk v1.16.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/labstack/gommon v0.3.0
	github.com/lib/pq v1.10.1
	github.com/loadoff/excl v0.0.0-20171207172601-c6a9e4c4b4c4
	github.com/magefile/mage v1.11.0 // indirect
	github.com/mattn/go-runewidth v0.0.12
	github.com/mattn/go-sqlite3 v1.14.7
	github.com/minio/pkg v1.0.11
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	github.com/sijms/go-ora/v2 v2.2.9
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/snowflakedb/gosnowflake v1.4.3
	github.com/spf13/cobra v1.2.1
	github.com/xo/dburl v0.7.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/image v0.0.0-20210504121937-7319ad40d33e
	golang.org/x/sys v0.0.0-20210927094055-39ccf1dd6fa6 // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/tools v0.1.7 // indirect
	google.golang.org/api v0.46.0
)

replace (
	github.com/dgrijalva/jwt-go => github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/miekg/dns => github.com/miekg/dns v1.1.43
	github.com/spf13/viper => github.com/spf13/viper v1.8.1
)
