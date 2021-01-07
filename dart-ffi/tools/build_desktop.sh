#!/bin/bash

cargo build --release

case $(uname | tr '[:upper:]' '[:lower:]') in
  linux*)
    mv "target/release/libisar_core_dart_ffi.so" "libisar_linux.so"
    ;;
  darwin*)
    mv "target/release/libisar_core_dart_ffi.dylib" "libisar_macos.dylib"
    ;;
  *)
    mv "target/release/isar_core_dart_ffi.dll" "isar_windows.dll"
    ;;
esac