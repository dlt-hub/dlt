#!/bin/bash
# Read poetry.lock and display information about dependencies:
#
# * Project dependencies
# * Sub-dependencies and reverse dependencies of packages
# * Summary of updates, or change in dependency versions between two revisions of the project
#
# Author: Sarunas Nejus, 2021
# License: MIT

helpstr="
   deps       [-lq] [<package>]
   deps  diff [<base-ref>=HEAD] [<target-ref>]

     -h, --help  -- display help

   deps  [-l]            -- show top-level project dependencies
          -l             -- include sub-dependencies

   deps  [-q] <package>  -- show direct and reverse dependencies for the package
          -q             -- exit with status 0 if package is a dependency, 1 otherwise

   deps  diff [<base-ref>=HEAD]  [<target-ref>]
                         -- summarise version changes between two revisions for all dependencies
                         -- it defaults to using a dirty poetry.lock in the current worktree
"

R=$'\033[0m'
B=$'\033[1m'
I=$'\033[3m'
RED=$'\033[1;38;5;204m'
GREEN=$'\033[1;38;5;150m'
YELLOW=$'\033[1;38;5;222m'
CYAN=$'\033[1;38;5;117m'
MAGENTA=$'\033[1;35m'
GREY=$'\033[1;38;5;243m'

lockfile=poetry.lock
pyproject=pyproject.toml

msg() {
  printf >&2 '%s\n' " · $*"
}

error() {
  echo
  msg "${RED}ERROR:$R $*"$'\n'
  exit 1
}

deps_diff () {
  echo
  if (( ! $# )) && git diff-files --quiet $lockfile; then
    msg "$B$lockfile$R is no different from the committed or staged version"
    exit
  fi

  git diff -U2 --word-diff=plain --word-diff-regex='[^. ]*' "$@" $lockfile |
    # remove the description line
    grep -v 'description = ' |
    # keep valid, *package* version *changes*
    grep '(^|[^{])version = .*(-\]|\+\})' -EC1 |
    # duplicate the version line to help with reporting in the next command
    sed '\/version/p; s/version/& green/' |
    sed -nr '
      # quit once we reach the files metadata section
      /\[metadata.files\]/q
      # get package name, make it bold and save it
      /name = /{ s///; s/.*/'"$B&$R"'/; h; }
      # get the first version line, remove the updated version and save it
      /version = /{ s///; s/\{\+[^+]+\+\}//g; s/$/ @->/; H; }
      # get the second version line, remove the old version and save it
      /version green = /{ s///; s/\[-[^-]+-\]//g; H; }
      /category = /{
        s///
        # append the category to the saved string
        H
        # take the saved string
        x
        # remove double quotes
        s/"//g
        # color all removals in red
        s/\[-([^]]*)-\]/'"$RED\1$R"'/g
        # color all additions in green
        s/\{\+([^+]+)\+\}/'"$GREEN\1$R"'/g
        s/\n/@/g
        p
      }
    ' | column -ts@
  echo
}

DEPS_COLOR_PAT='
  s/^/\t/
  # s/=([<>])/ \1/
  s/([a-z_=-]+=)([^@]+)/\n\t @ \1'"$R$B$I\2$R"'/g
  s/(rich.tables|beetcamp|beets)[^ \t]*/'"$CYAN\1$R"'/g
  s/([ ,])([<0-9.]+[^@ ]+|[*]|$)/\1'"$RED\2$R"'/g
  /[>~^]=?/{
    /[<~^]+/s/([>~^][0-9.=a-z]+)/'"$YELLOW&$R"'/g
    /</!s/(>[^ @]+)/'"$GREEN&$R"'/
  }
'
_requires() {
  pkg=$1
  sed -nr '
    # take paragraphs starting with queried package name, until next one
    /^name = "('"$pkg"')"/I,/\[\[package\]\]/{
      # its dependencies section, until next empty line
      s/name = "([^"]+)"/    '"$B\1$R"' requires/p
      /\[package.dep.*/,/^$/{
        # ignore header
        //d
        # remove double quotes and other markers
        s/, .*| = \{ ?version|"|==//g
        s/ =|$/ @/
        '"$DEPS_COLOR_PAT"'
        p
      }
    }
  ' $lockfile | column -ts@
}

_required_by() {
  pkg=$1
  echo "    $B$pkg$R is required by"
  sed -nr '
    # memorise current package name
    /^name = "([^"]+)"/{ s//\1/; h; }
    /^["]?'"$pkg"'["]? = (.version = )?"([^"]+)"([^"]+"(([\\]["]|[^"])+)".+)?/I{
      # append the queried package _version_ (first group) to the name that was memorised
      s//\2@\4/
      s/ ([<>=]) /=\1/
      s/[\\"]//g; H
      # retrieve name and version from the memory and
      # split them with an @. finally, colorize
      x; s/\n/ @ /
      '"$DEPS_COLOR_PAT"'
      p
    }
  ' $lockfile | column -ts@
}

_project_deps_deps() {
  section=dependencies
  [[ $1 == dev ]] && section=dev-dependencies
  maindeps="($(sed -rn '
    /tool.poetry.'"$section"'/,/^\[/{
      //d
      /^python/d
      s/^([^ ]+) =.*/\1/p
    }
  ' pyproject.toml | paste -sd'|'))"
  _requires "$maindeps"
}

_project_deps() {
  section=dependencies
  [[ $1 == dev ]] && section=dev-dependencies
  sed -rn '
    /tool.poetry.'"$section"'/,/^\[/{
      //d
      # skip empty lines, python version and comments
      /^($|(python|#).*$)/d
      # remove irrelevant characters and comments
      s/[]\[{}" ]|#.+//g
      # when requirement is an object, join members with @
      /,([^=]+=)/{
        s//@ \1/g
        # version requirement is already clear
        s/version=//g
      }
      # split package name and its requirements
      s/=/ @ /
      '"$DEPS_COLOR_PAT"'
      p
    }
  ' $pyproject | column -ts@
}

show_help() {
  sed -r '
    ### Comments
    s/-- .*/'"$GREY&$R"'/

    ### Optional arguments
    # within brackets
    s/(\W)(-?-(\w|[-])+)/\1'"$B$YELLOW\2$R"'/g

    ### Commands
    /^(  +)([_a-z][^ A-Z]*)(  +|\t| *$)/s//\1'"$B$CYAN\2$R"'\3/

    # <arg>
    /<[^>]+>/s//'"$B$MAGENTA&$R"'/g

    ### Default values
    # =arg|=ARG
    /=((\w|-)+)/s//='"$B$GREEN\1$R"'/g

    ### Punctuation
    s/(\]+)( |$)/'"$B$YELLOW\1$R"'\2/g
    s/([m ])(\[+)/\1'"$B$YELLOW\2$R"'/g

  ' <<< "$helpstr"
}

if [[ " $* " =~ \ (--help|-h|help)\  ]]; then
  show_help
  exit
fi

[[ -r $lockfile ]] || error $lockfile is not found in the working directory

if [[ $1 == diff ]]; then
  deps_diff "${@:2}"
  exit
fi

[[ " $* " == *" -q "* ]] && quiet=1
[[ " $* " == *" -l "* ]] && long=1
_pkg=(${@#-q})
_pkg=(${@#-l})
pkg=${_pkg[0]}

while read -r line; do
  if [[ $line =~ ^name.=.\"([^\"]+)\" ]]; then
    project=${BASH_REMATCH[1]}
  elif [[ $line =~ ^version.=.\"([^\"]+)\" ]]; then
    version=${BASH_REMATCH[1]}
  elif [[ $line =~ ^python.=.\"([^\"]+)\" ]]; then
    python_req=${BASH_REMATCH[1]}
    break
  fi
done < $pyproject

if (( ! quiet )); then
  msg "Project: $B$project$R"
  msg "Version: $B$version$R"
  msg "Python:  $B$python_req$R"
fi
if (( ! ${#pkg} )); then
  if (( long )); then
    func=_project_deps_deps
  else
    func=_project_deps
  fi
  echo && echo "$B MAIN DEPENDENCIES$R"
  $func
  echo && echo "$B DEV DEPENDENCIES$R"
  $func dev
  echo
else
  pkg_with_ver=$(\
    sed -rn '
      /^name = "('"$pkg"')"/I{ s//\1/; h; }
      /^version = "(.+)"/{
        s//\1/; H;
        x; /'"$pkg"'/I{ s/\n/ /; p; q; }
      }
    ' $lockfile)
  if (( quiet )); then
    # checking existence
    [[ -n $pkg_with_ver ]]
  else
    [[ -n $pkg_with_ver ]] || error "Package $B$pkg$R is not found"
    msg "Package: $B$pkg_with_ver$R"
    echo
    _requires "$pkg"
    echo
    _required_by "$pkg"
    echo
  fi
fi