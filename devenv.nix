{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.gnumake
  ];

  # https://devenv.sh/languages/
  languages.python.enable = true;
  languages.python.poetry.enable = true;
  languages.python.poetry.activate.enable = true;
  # languages.python.version = "3.8";

  # https://devenv.sh/scripts/
  scripts.hello.exec = ''
    echo "Run 'setup' to prepare this repository for development"
  '';
  scripts.setup.exec = ''
    make dev
    make help
  '';

  enterShell = ''
    hello
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
    make has-poetry
  '';

  # https://devenv.sh/pre-commit-hooks/
  git-hooks.hooks.lint = {
    enable = true;

    # The name of the hook (appears on the report table):
    name = "Lint";

    # The command to execute (mandatory):
    entry = "make lint";

    # The language of the hook - tells pre-commit
    # how to install the hook (default: "system")
    # see also https://pre-commit.com/#supported-languages
    language = "system";

    # Set this to false to not pass the changed files
    # to the command (default: true):
    pass_filenames = false;
  };

  # See full reference at https://devenv.sh/reference/options/
}
