if [ -d build ]
then
    git co -B gh-pages
    touch build/sphinx/html/.nojekyll
    git add build --force
    git commit -m 'add build'
    git push origin :gh-pages
    git subtree push --prefix build/sphinx/html origin gh-pages
    git co -
else
    echo "build the documentation first with $> python setup.py docs"
fi
