package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Address  []string
	LeaderId int
}

func LoadDefaultConfig() (*Config, error) {
	return LoadConfigFromFile("config.json")
}

func LoadConfigFromFile(path string) (*Config, error) {
	config := new(Config)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(config)
	if err != nil {
		return nil, err
	}
	return config, nil
}