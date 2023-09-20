git checkout -B docs-regen
cd integration_tests
dbt docs generate
mv -f target/*.json ../docs
mv -f target/*.html ../docs
git add .
git commit -am"updating docs site"
git push --set-upstream origin docs-regen
git checkout main
git branch -D docs-regen
