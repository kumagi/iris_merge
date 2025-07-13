# Project Build Instructions

This project uses CMake and Ninja for building.

## Prerequisites

- CMake
- Ninja (`ninja-build`)
- A C++17 compliant compiler

## Building

1.  Create a build directory:
    ```bash
    mkdir build
    ```

2.  Change into the build directory:
    ```bash
    cd build
    ```

3.  Configure the project with CMake, specifying Ninja as the generator:
    ```bash
    cmake -G Ninja ..
    ```

4.  Build the project with Ninja:
    ```bash
    ninja
    ```

## Notice

This project comply Google C++ style guide. So that:

- Exceptions are never admitted.

- Using `auto` is limited for cases below:
  - Iterator.
  - Right hand side of the assignment explicitly specify the type name.

- Every `class` member fields have `_` suffix.
- `struct` does not need the suffix.
- Every functions are CamelCase, variables names are snake_case.
