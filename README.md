# KGax

Google API extensions for Kotlin.

[![CircleCI](https://circleci.com/gh/googleapis/gax-kotlin/tree/master.svg?style=svg)](https://circleci.com/gh/googleapis/gax-kotlin/tree/master)
[![codecov](https://codecov.io/gh/googleapis/gax-kotlin/branch/master/graph/badge.svg)](https://codecov.io/gh/googleapis/gax-kotlin)

KGax is a small set of utility libraries for interacting with generated [gRPC](https://grpc.io/) Java stubs in Kotlin.  
It may be used directly to make interacting with the gRPC stubs easier in Kotlin, or it can be combined with 
a code generator to produce higher-level client libraries leveraging gRPC. 

KGax currently includes:

  1. **kgax-core**: Common dependencies for the other modules below.
  1. **kgax-grpc**: Utilities for using gRPC Java stubs in Kotlin.

## Usage

Refer to the examples in the `kgax-grpc-examples` directory.

## Contributing

Contributions to this library are always welcome and highly encouraged.

See the [CONTRIBUTING](CONTRIBUTING.md) documentation for more information on how to get started.

## Versioning

This library is currently a *preview* with no guarantees of stability or support. Please get involved and let us know
if you find it useful and we'll work towards a stable version.

## Disclaimer

This is not an official Google product.
