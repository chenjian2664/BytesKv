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

package storage

import (
	"BytesDB/config"
)

// configurations
// should be kept in metadata

// reserved configs:
//
// bytes.schema.path.public the default path that table will belong to `public` schema
// bytes.schema.type.public the default type that will create under `public` schema
// bytes.schema.per.max-size.public the default maxSize(bytes) for each storage unit

type StorageOptions struct {
	// warehouse directory
	rootPath string
}

// FromDbOptions pure and validate config for storage
func FromDbOptions(cfg *config.DBConfig) *StorageOptions {
	return &StorageOptions{
		rootPath: cfg.DataDir,
	}
}
