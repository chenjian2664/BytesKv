#!/bin/bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

find "$PROJECT_ROOT" -name "*.go" -type f | while read -r file; do
    read -r -d '' LICENSE << EOF
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
*/\n

EOF

    if ! grep -q "Apache License" "$file"; then
        echo "$LICENSE" | cat - "$file" > temp && mv temp "$file"
        # Ensure there's a blank line after the license header
        sed -i.bak '/^package/i\\' "$file" && rm "${file}.bak"
        echo "Added license to $file"
    fi
done