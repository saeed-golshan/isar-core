#!/bin/bash

cargo build --release

case $(uname | tr '[:upper:]' '[:lower:]') in
  linux*)
    mv "target/release/libisar_core.so" "libisar_linux.so"
    ;;
  darwin*)
    mv "target/release/libisar_core.dylib" "libisar_macos.dylib"
    ;;
  *)
    mv "target/release/isar_core.dll" "isar_windows.dll"
    ;;
esac