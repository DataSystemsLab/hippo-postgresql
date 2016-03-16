/**
 *   Copyright 2011 Garrick Toubassi
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <iostream>
#include <fstream>
#include <femtozip/FileUtil.h>

using namespace std;

namespace femtozip {

const char *FileUtil::readFully(const char *path, int& length) {
    ifstream file(path, ios::in | ios::binary | ios::ate);
    if (file.is_open()) {
        streampos size = file.tellg();
        char *buf = new char[size];
        file.seekg(0, ios::beg);
        file.read(buf, size);
        file.close();

        length = size;
        return buf;
    }
    length = 0;
    return 0;
}

}
