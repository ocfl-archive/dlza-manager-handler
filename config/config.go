package config

import (
	"io/fs"
	"os"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/stashconfig"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

type HandlerConfig struct {
	LocalAddr               string            `toml:"localaddr"`
	Domains                 []string          `toml:"domains"`
	ExternalAddr            string            `toml:"externaladdr"`
	Bearer                  string            `toml:"bearer"`
	ResolverAddr            string            `toml:"resolveraddr"`
	ResolverTimeout         config.Duration   `toml:"resolvertimeout"`
	ResolverNotFoundTimeout config.Duration   `toml:"resolvernotfoundtimeout"`
	ActionTimeout           config.Duration   `toml:"actiontimeout"`
	ServerTLS               *loader.Config    `toml:"server"`
	ClientTLS               *loader.Config    `toml:"client"`
	GRPCClient              map[string]string `toml:"grpcclient"`
	DBConn                  config.EnvString  `toml:"dbconn"`
	Addresses               map[string]string `toml:"addresses"`
	Netname                 string            `toml:"netname"`

	Log      stashconfig.Config `toml:"log"`
	Database DatabaseConfig     `toml:"database"`
}

func LoadHandlerConfig(fSys fs.FS, fp string, conf *HandlerConfig) error {
	if _, err := fs.Stat(fSys, fp); err != nil {
		path, err := os.Getwd()
		if err != nil {
			return errors.Wrap(err, "cannot get current working directory")
		}
		fSys = os.DirFS(path)
		fp = "handler.toml"
	}
	data, err := fs.ReadFile(fSys, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot read file [%v] %s", fSys, fp)
	}
	_, err = toml.Decode(string(data), conf)
	if conf.Database.Password == "" {
		conf.Database.Password = os.Getenv("DB_PASSWORD")
	}
	if err != nil {
		return errors.Wrapf(err, "error loading config file %v", fp)
	}
	return nil
}
