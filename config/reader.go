/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LoadConfig reads the db.properties file and returns a DBConfig
func LoadConfig(configPath string) (*DBConfig, error) {
	if configPath == "" {
		configPath = "db.properties"
	}

	// Use default config if file doesn't exist
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return &DBConfig{
			DataDir:     "/tmp/bytesdb",
			MaxFileSize: 1048576, // 1MB
			IndexType:   "hash",
		}, nil
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &DBConfig{}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "data.dir":
			config.DataDir = filepath.Clean(value)
		case "storage.file.max.size":
			if size, err := strconv.ParseInt(value, 10, 64); err == nil {
				config.MaxFileSize = size
			}
		case "index.type":
			config.IndexType = value
		case "storage.type":
			config.IndexType = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Set defaults for empty values
	if config.DataDir == "" {
		config.DataDir = "/tmp/bytesdb"
	}
	if config.MaxFileSize == 0 {
		config.MaxFileSize = 1048576 // 1MB
	}
	if config.IndexType == "" {
		config.IndexType = "local_hash"
	}
	if config.StorageType == "" {
		config.StorageType = "local_file"
	}

	return config, nil
}
