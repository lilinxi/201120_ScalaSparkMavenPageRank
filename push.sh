cd $(dirname $0)
git add .
time=$(date "+%Y-%m-%d %H:%M:%S")
git commit -m"commit at ${time}"
git push