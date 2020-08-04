package config

import (
	"fmt"
	"runtime"

	log "github.com/sirupsen/logrus"

	"github.com/nomon/gonfig"

	_ "net/http/pprof"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	ListenPort    int    `env:"CHUPD_LISTENPORT"`
	TestFlag      bool   `env:"CHUPD_TESTFLAG"`
	FlushInterval int    `env:"CHUPD_INTERVAL"`
	FlushCount    int    `env:"CHUPD_COUNT"`
	DbHost        string `env:"CHUPD_DBHOST"`
	DbName        string `env:"CHUPD_DBNAME"`
	DbUser        string `env:"CHUPD_DBUSER"`
	DbPass        string `env:"CHUPD_DBPASS"`
	DbPort        int    `env:"CHUPD_DBPORT"`
	CHUrl         string `env:"CHUPD_CHURL"`
}

type Env struct {
	Db       *sqlx.DB
	Config   *Config
	loglevel log.Level
}

func NewEnv(path string) *Env {

	conf := gonfig.NewConfig(nil)
	conf.Set("always", true);
	conf.Use("argv"), NewNewArgvConfig("myapp.*"))


	var cfg Config
	cfg := gonfig.NewJsonConf(path+"/"+"conf.json")
	if err != nil {
		cfg = gonfig.NewJsonConf(path+"/"+"conf.tpl.json", &cfg)
		checkErr(err)
	}

	loglevel := log.WarnLevel

	return &Env{
		Config:   &cfg,
		loglevel: loglevel,
	}
}

func (e *Env) InitLog() {
	if e.Config.TestFlag {
		e.loglevel = log.DebugLevel
	}

	log.SetLevel(e.loglevel)
	log.SetFormatter(&log.JSONFormatter{})
}

func (e *Env) InitDb() {
	dsn := initDbDsn(e.Config)
	log.Debug(dsn)
	db, err := sqlx.Connect("clickhouse", dsn)
	checkErr(err)
	e.Db = db
}

func initDbDsn(cfg *Config) string {
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s", cfg.DbHost, cfg.DbPort, cfg.DbUser, cfg.DbPass, cfg.DbName)
}

func checkErr(err error) {
	if err != nil {
		_, filename, lineno, ok := runtime.Caller(1)
		message := ""
		if ok {
			message = fmt.Sprintf("%v:%v: %v\n", filename, lineno, err)
		}
		log.Panic(message, err)
	}
}
