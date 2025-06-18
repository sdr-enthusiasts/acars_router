# in flake.nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          libPath = with pkgs; lib.makeLibraryPath [
            libGL
            libxkbcommon
            wayland
          ];
          rustToolchain = pkgs.rust-bin.stable.latest.default;
          # new! 👇
          nativeBuildInputs = with pkgs; [ rustToolchain ];
          # also new! 👇
          buildInputs = with pkgs; [ cargo-make cargo-deny cargo-machete cargo-profiler samply cargo-tauri typos ];
          RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
          LD_LIBRARY_PATH = libPath;
        in
        with pkgs;
        {
          devShells.default = mkShell {
            # 👇 and now we can just inherit them
            inherit buildInputs nativeBuildInputs RUST_SRC_PATH LD_LIBRARY_PATH;
          };
        }
      );
}

# https://www.reddit.com/r/rust/comments/mmbfnj/nixifying_a_rust_project/
