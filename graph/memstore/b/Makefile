# Copyright 2014 The Cayley Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: specify 

# Do not commit changes to this line unless you are satisfied
# that the github.com/cznic/b tests AND the cayley integration
# tests pass with the new sha.
pinned=82d9e96a4503a42315b0fdf5201314302beafe06

specify:
	rm -rf b
	git clone https://github.com/cznic/b
	cd b && git checkout $(pinned)
	go test ./b
	@sed -e 's|interface{}[^{]*/\*K\*/|int64|g' -e 's|interface{}[^{]*/\*V\*/|struct{}|g' b/btree.go >keys.go
	rm -rf b
