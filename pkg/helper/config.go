package helper

import (
	_ "embed"
	"encoding/json"
)

//go:embed configs.json
var configData []byte

type ClientConfig struct {
	ConnectionTimeout     int64 `json:"ConnectionTimeout"`
	RequestTimeout        int64 `json:"RequestTimeout"`
	ReadRetries           int64 `json:"ReadRetries"`
	WriteRetries          int64 `json:"WriteRetries"`
	MaxConcurrency        int64 `json:"MaxConcurrency"`
	MaxPendingConnections int64 `json:"MaxPendingConnections"`
}

type TrafficConfig struct {
	ItemSizes          map[string]int64 `json:"ItemSizes"`
	NumberOfAttributes int64            `json:"NumberOfAttributes"`
	APIs               []string         `json:"APIs"`
	CacheHitPercentage int64            `json:"CacheHitPercentage"`
}

type TestConfig struct {
	Name          string `json:"Name"`
	Description   string `json:"Description"`
	ClientConfig  string `json:"ClientConfig"`
	Reboot        int64  `json:"Reboot"`
	TrafficConfig string `json:"TrafficConfig"`
	Endpoint      string `json:"Endpoint"`
}

type AppConfig struct {
	ClientConfigs  map[string]ClientConfig  `json:"ClientConfigs"`
	TrafficConfigs map[string]TrafficConfig `json:"TrafficConfigs"`
	TestConfigs    map[string]TestConfig    `json:"TestConfigs"`
	Table          string                   `json:"Table"`
}

func GetAppConfig() AppConfig {
	out := AppConfig{}

	if err := json.Unmarshal(configData, &out); err != nil {
		panic(err)
	}

	return out
}
