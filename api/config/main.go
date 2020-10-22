package config

import (
	"fmt"
	"runtime"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/viper"

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
	viper.SetConfigType("json")
	viper.SetConfigName(path)
	viper.AddConfigPath("config")

	if err := viper.ReadInConfig(); err != nil {
		checkErr(err)
	}

	var cfg Config
	err := viper.Unmarshal(&cfg)
	if err != nil {
		checkErr(err)
	}

	loglevel := log.WarnLevel
	if cfg.TestFlag {
		loglevel = log.DebugLevel
	}

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
