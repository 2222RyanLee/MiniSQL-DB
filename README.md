# MiniSQL

This framework is a modified version based on the CMU-15445 BusTub framework. It retains some core design principles of the buffer pool, index, and record modules, while making some modifications and extensions to make it compatible with the requirements of the original MiniSQL lab guide.

The following are the major modifications/extensions:

- Modified the Disk Manager module to support persistent data page allocation and deallocation status by extending bitmap pages and disk file metadata pages.
- Implemented data persistence by serializing and deserializing memory objects in modules such as Record Manager, Index Manager, and Catalog Manager.
- Refactored data structures (`Row`, `Field`, `Schema`, `Column`, etc.) and implementations in the Record Manager layer.
- Restructured the Catalog Manager layer to support persistence and provide interface calls for the Executor layer.
- Expanded the Parser layer to support outputting syntax trees for Executor layer invocation.

In addition, there are many miscellaneous changes, including adjustments to some module code in the source code, modifications to test code, performance optimizations, etc., which are not elaborated here.

Note: To avoid code plagiarism, please do not publish your code on any public platform.

### Compilation & Development Environment
- Apple clang version: 11.0+ (MacOS), use `gcc --version` and `g++ --version` to check
- gcc & g++: 8.0+ (Linux), use `gcc --version` and `g++ --version` to check
- cmake: 3.20+ (Both), use `cmake --version` to check
- gdb: 7.0+ (Optional), use `gdb --version` to check
- flex & bison (not required for now, but needed if you want to modify the syntax of the SQL compiler)
- llvm-symbolizer (not required for now)
  - For macOS: `brew install llvm`, then set the path and environment variables.
  - For CentOS: `yum install devtoolset-8-libasan-devel libasan`
  - Reference: [Google sanitizers with CLion](https://www.jetbrains.com/help/clion/google-sanitizers.html#AsanChapter), [Chinese blog post](https://www.jianshu.com/p/e4cbcd764783)

### Build
#### Windows
Currently, this code does not support compilation on the Windows platform. However, on Windows 10 and above, you can use the Windows Subsystem for Linux (WSL) to develop and build. Choose the Ubuntu subsystem for WSL (Ubuntu 20 or above is recommended). If you use CLion as your IDE, you can configure WSL in CLion for debugging. For details, please refer to [Clion with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends/).

#### MacOS & Linux & WSL
Basic build commands:
```bash
mkdir build
cd build
cmake ..
make -j
```
If there are no changes to the `CMakeLists` related files and no new or deleted `.cpp` code (in other words, only modifications to existing code), there is no need to execute the `cmake ..` command again. Just execute `make -j` to compile.

The default compilation mode is `debug`. If you want to compile in `release` mode, use the following command:
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### Testing
After building, the `minisql_test` executable will be generated in the `build/test` directory. Run `./minisql_test` to execute all tests.

To run a single test, for example

, if you want to run the test file corresponding to `lru_replacer_test.cpp`, use the command `make lru_replacer_test` to build it.