package config

import "embed"

//go:embed handler.toml
var ConfigFS embed.FS
