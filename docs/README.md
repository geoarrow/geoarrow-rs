# Top-level documentation website

Mkdocs-based website to serve as high-level website and refer people to language-specific documentation.

To build website:

```
uv run mkdocs serve
```

To deploy: We have a couple manual steps because `mkdocs gh-deploy` _replaces_
any existing content on the `gh-pages` branch and we want an _upsert_ that
doesn't touch the `js/` or `python/` directories, which are deployed separately.

```bash
uv run mkdocs build
git checkout gh-pages
cd ..
git pull
rm -rf 404.html assets index.html sitemap.xml sitemap.xml.gz search stylesheets rust
mv -f docs/site/* ./
git add 404.html assets index.html sitemap.xml sitemap.xml.gz search stylesheets rust
git commit -m "New revision of top-level docs site"
git push
# Return to previous branch
git checkout -
```
