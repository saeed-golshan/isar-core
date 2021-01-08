#!/bin/bash

if [[ "$(uname -s)" == "Darwin" ]]; then
    export NDK_HOST_TAG="darwin-x86_64"
elif [[ "$(uname -s)" == "Linux" ]]; then
    export NDK_HOST_TAG="linux-x86_64"
else
    echo "Unsupported OS."
    exit
fi

NDK=${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-"$ANDROID_SDK_ROOT/ndk"}}
COMPILER_DIR="$NDK/toolchains/llvm/prebuilt/$NDK_HOST_TAG/bin"
export PATH="$COMPILER_DIR:$PATH"

echo "$COMPILER_DIR"

if [ "$1" = "x64" ]; then
  cp "$COMPILER_DIR/x86_64-linux-android29-clang" "$COMPILER_DIR/x86_64-linux-android-clang"
  rustup target add x86_64-linux-android

  export CARGO_TARGET_X86_64_LINUX_ANDROID_AR="$COMPILER_DIR/x86_64-linux-android-ar"
  export CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER="$COMPILER_DIR/x86_64-linux-android-clang"

  cargo build --target x86_64-linux-android --release
  mv "target/x86_64-linux-android/release/libisar_core_dart_ffi.so" "libisar_android_x64.so"
else
  cp "$COMPILER_DIR/aarch64-linux-android29-clang" "$COMPILER_DIR/aarch64-linux-android-clang"
  rustup target add aarch64-linux-android

  export CARGO_TARGET_AARCH64_LINUX_ANDROID_AR="$COMPILER_DIR/aarch64-linux-android-ar"
  export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$COMPILER_DIR/aarch64-linux-android-clang"

  cargo build --target aarch64-linux-android --release
  mv "target/aarch64-linux-android/release/libisar_core_dart_ffi.so" "libisar_android_arm64.so"
fi






