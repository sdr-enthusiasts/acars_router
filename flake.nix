{
  description = "Consumer repo using shared base + rust precommit system";

  inputs = {
    precommit.url = "github:FredSystems/pre-commit-checks";
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs =
    {
      self,
      precommit,
      nixpkgs,
      ...
    }:
    let
      systems = precommit.lib.supportedSystems;
    in
    {
      ##########################################################################
      ## CHECKS — unified base+rust via mkCheck
      ##########################################################################
      checks = builtins.listToAttrs (
        map (system: {
          name = system;
          value = {
            pre-commit-check = precommit.lib.mkCheck {
              inherit system;
              src = ./.;
              check_rust = true;
              enableXtask = false;
              extraExcludes = [
                "typos.toml"
              ];
            };
          };
        }) systems
      );

      ##########################################################################
      ## DEV SHELLS — merged env + your extra Rust goodies
      ##########################################################################
      devShells = builtins.listToAttrs (
        map (system: {
          name = system;

          value =
            let
              pkgs = import nixpkgs { inherit system; };

              # Unified check result (base + rust)
              chk = self.checks.${system}."pre-commit-check";

              # Packages that git-hooks.nix / mkCheck say we need
              corePkgs = chk.enabledPackages or [ ];

              # Extra Rust / tooling packages (NO extra rustc here).
              # `chk.passthru.devPackages` is intentionally NOT listed here —
              # it is a list, and embedding it inline produces a nested list
              # in `buildInputs` (deprecated as of nixpkgs 26.05). It is
              # already pulled in via `extraDev` below and concatenated with
              # `++` in `buildInputs`.
              extraRustTools = [
                pkgs.cargo-deny
                pkgs.cargo-machete
                pkgs.cargo-make
                pkgs.cargo-profiler
                pkgs.cargo-bundle
                pkgs.typos
                pkgs.vttest
                pkgs.markdownlint-cli2
              ];

              # Extra dev packages provided by mkCheck (includes rustToolchain)
              extraDev = chk.passthru.devPackages or [ ];

              # Library path packages: whatever mkCheck wants + your GL/Wayland bits
              libPkgs = chk.passthru.libPath or [ ];
            in
            {
              default = pkgs.mkShell {
                buildInputs = extraDev ++ corePkgs ++ extraRustTools;

                LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath libPkgs;

                shellHook = ''
                  ${chk.shellHook}

                  alias pre-commit="pre-commit run --all-files"
                '';
              };
            };
        }) systems
      );
    };
}
