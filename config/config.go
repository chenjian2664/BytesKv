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

// DBConfig holds all database configuration
type DBConfig struct {
	// Root directory for database files
	DataDir string `properties:"data.dir,default=/tmp/bytesdb"`

	// Maximum size for a single storage file (in bytes)
	MaxFileSize int64 `properties:"storage.file.max.size,default=1048576"` // default 1MB

	// Index type
	IndexType string `properties:"index.type,default=local_hash"`

	// Storage type
	StorageType string `properties:"storage.type,default=local_file"`
}
