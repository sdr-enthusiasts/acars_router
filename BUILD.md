# How to build the project

## Docker

Building with Docker is super easy.

```shell
docker build -f Dockerfile.local . -t name, organization, or whatever you like < your > /acars_router:test
```

And the project should build with no issues.

## Building acars_router from source

If you desire to build `acars_router` from source, ensure you have [rust](https://www.rust-lang.org/tools/install) installed and up to date. `acars_router` build target is always the most recent Rust version, so you should not have to roll back to previous versions to get it built.

Once you clone the repository and enter the repo's directory using a shell...

Debugging:

```shell
cargo build
```

Release

```shell
cargo build --release
```

To run the project using cargo (useful for debugging):

```shell

cargo run -- <command line flags here>
```

Or you can directly run/find the binary in the `target` directory.
