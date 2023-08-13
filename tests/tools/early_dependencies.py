# import requests
# import toml
# from datetime import datetime, timedelta

# def get_old_versions(package_name, days_old=530):
#     """Return versions of package_name that are older than days_old."""
#     url = f"https://pypi.org/pypi/{package_name}/json"
#     response = requests.get(url)
#     response.raise_for_status()

#     data = response.json()
#     releases = data.get("releases", {})

#     threshold_date = datetime.now() - timedelta(days=days_old)
#     old_versions = []

#     for version, release_data in releases.items():
#         # The release data contains info for each distribution type (e.g., wheel, sdist)
#         # We'll use the first one's upload time as a reference.
#         if release_data:
#             upload_time = datetime.fromisoformat(release_data[0]["upload_time_iso_8601"][:-1])  # Strip 'Z'
#             if upload_time < threshold_date:
#                 old_versions.append(version)

#     return old_versions

# def extract_min_version(version_spec):
#     """Extract and return the minimal version from a version spec."""
#     if isinstance(version_spec, str):
#         # Common patterns: >=X.Y, ^X.Y, ~X.Y, X.Y.*, etc.
#         # Split by common delimiters and take the version part
#         for delimiter in ['>=', '^', '~', '==', '>', '<=', '<']:
#             if delimiter in version_spec:
#                 return version_spec.split(delimiter)[-1].strip()
#         return None
#     elif isinstance(version_spec, dict):
#         # If the specification is a dictionary, handle git or other cases
#         if "version" in version_spec:
#             return extract_min_version(version_spec["version"])
#         if "git" in version_spec:
#             # For git dependencies, you might return the branch, commit, or tag
#             return version_spec.get("branch", version_spec.get("rev", version_spec.get("tag")))
#     return None

# def main():
#     with open("pyproject.toml", "r") as f:
#         data = toml.load(f)

#     dependencies = data.get("tool", {}).get("poetry", {}).get("dependencies", {})
#     # Removing python as it's a special dependency
#     dependencies.pop("python", None)

#     old_deps = {}
#     for dep, version_spec in dependencies.items():
#         # print(dep)
#         # print(version_spec)
#         # if isinstance(version_spec, dict) and "optional" in version_spec:
#         #     print("skipping opt")
#         #     continue
#         # old_versions = get_old_versions(dep)
#         # if old_versions:
#         #     # Use the oldest version among the old versions
#         #     old_deps[dep] = old_versions[-1]
#         old_deps[dep] = extract_min_version(version_spec)

#     # Write to requirements.txt
#     with open("min_requirements.txt", "w") as f:
#         for dep, version in old_deps.items():
#             f.write(f"{dep}=={version}\n")

#     print("Written old dependencies to requirements.txt")

# if __name__ == "__main__":
#     main()
