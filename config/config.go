package config

import (
	"github.com/spf13/viper"
)

var RDBDir string

func init() {
	viper.SetConfigName("config")
	viper.SetDefault("RDB_DIR", "./data")
	viper.AutomaticEnv()

	RDBDir = viper.GetString("RDB_DIR")
}
