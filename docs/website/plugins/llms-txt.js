/**
 * Custom Docusaurus plugin that generates .md files and llms.txt from
 * preprocessed source markdown.
 *
 * Instead of converting built HTML back to markdown (like the SignalWire
 * plugin), this copies the already-preprocessed source .md files into the
 * build output and generates llms.txt from their frontmatter.
 *
 * MDX/React imports and self-closing component tags are stripped during
 * copy since they are navigation/UI widgets, not content.
 */
const fs = require('fs');
const path = require('path');

// --- Helpers ---------------------------------------------------------------

/** Recursively collect files matching a predicate. */
function walkFiles(dir, predicate, results = []) {
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkFiles(full, predicate, results);
    } else if (predicate(entry.name)) {
      results.push(full);
    }
  }
  return results;
}

/** Read YAML frontmatter from a markdown string. Returns {title, description, slug}. */
function readFrontmatter(content) {
  const match = content.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  if (!match) return {};
  const fm = {};
  for (const line of match[1].split('\n')) {
    const m = line.match(/^(title|description|slug)\s*:\s*(.+)/);
    if (m) {
      // Strip surrounding quotes if present
      fm[m[1]] = m[2].replace(/^['"]|['"]$/g, '').trim();
    }
  }
  return fm;
}

/**
 * Build a map from "dir/slug" -> source file path for files with custom slugs.
 * This handles the case where slug: in frontmatter changes the URL path.
 */
function buildSlugMap(sourceDir) {
  const slugMap = {};
  const mdFiles = walkFiles(sourceDir, (name) => name.endsWith('.md') || name.endsWith('.mdx'));
  for (const file of mdFiles) {
    const content = fs.readFileSync(file, 'utf8');
    const fm = readFrontmatter(content);
    if (fm.slug) {
      const rel = path.relative(sourceDir, path.dirname(file));
      const key = rel ? `${rel}/${fm.slug}` : fm.slug;
      slugMap[key.split(path.sep).join('/')] = file;
    }
  }
  return slugMap;
}

/**
 * Clean MDX/React artifacts from markdown source.
 *
 * - Strips `import ... from '...'` lines (MDX imports)
 * - Strips single-line self-closing React component tags: <Component />
 * - Strips multi-line self-closing React component tags that start with
 *   <Component on one line and end with /> on a later line
 */
function cleanMarkdown(content) {
  const lines = content.split('\n');
  const result = [];
  let insideComponent = false;

  for (const line of lines) {
    // Strip MDX import lines: import X from '...' or import X from "..."
    if (/^import\s+.+\s+from\s+['"]/.test(line)) {
      continue;
    }

    // If we're inside a multi-line self-closing component, skip until />
    if (insideComponent) {
      if (/\/>\s*$/.test(line)) {
        insideComponent = false;
      }
      continue;
    }

    // Single-line self-closing React component: <Component ... />
    if (/^\s*<[A-Z][A-Za-z]*[\s/]/.test(line) && /\/>\s*$/.test(line)) {
      continue;
    }

    // Start of multi-line self-closing component: <Component ...  (no closing yet)
    if (/^\s*<[A-Z][A-Za-z]*[\s]/.test(line) && !/>\s*$/.test(line)) {
      insideComponent = true;
      continue;
    }

    result.push(line);
  }

  return result.join('\n');
}

// --- Plugin ----------------------------------------------------------------

/**
 * Build a version routing table from the Docusaurus docs plugin config.
 *
 * Each entry maps a URL path prefix to:
 *   - sourceDir: where the .md source files live
 *   - isMaster: whether this is the master (latest) version
 *   - prefix: the URL path prefix ('' for root-mounted master)
 *
 * Versions:
 *   - "current" (devel): path 'devel', source in docs_processed/
 *   - "master": path '/', source in versioned_docs/version-master/
 *   - other versions (e.g. "1.5.0"): path = version name,
 *     source in versioned_docs/version-{name}/
 */
function buildVersionRoutes(siteDir, docsVersions) {
  const routes = [];

  // "current" version (devel) — always uses docs_processed/
  const currentCfg = docsVersions['current'] || {};
  const currentPrefix = (currentCfg.path || 'devel').replace(/^\/+/, '');
  routes.push({
    prefix: currentPrefix ? currentPrefix + '/' : '',
    sourceDir: path.join(siteDir, 'docs_processed'),
    isMaster: false,
  });

  // Read versions.json for named versions
  const versionsJsonPath = path.join(siteDir, 'versions.json');
  let knownVersions = [];
  if (fs.existsSync(versionsJsonPath)) {
    knownVersions = JSON.parse(fs.readFileSync(versionsJsonPath, 'utf8'));
  }

  for (const version of knownVersions) {
    const vCfg = docsVersions[version] || {};
    const vPath = (vCfg.path || version).replace(/^\/+/, '');
    const vSourceDir = path.join(siteDir, 'versioned_docs', `version-${version}`);

    routes.push({
      prefix: vPath ? vPath + '/' : '', // master has path='/' → prefix=''
      sourceDir: vSourceDir,
      isMaster: version === 'master',
    });
  }

  // Sort: longer prefixes first so "devel/" matches before "" (root)
  routes.sort((a, b) => b.prefix.length - a.prefix.length);
  return routes;
}

module.exports = function llmsTxtPlugin(_context, options) {
  const {
    siteTitle = 'dlt - data load tool',
    siteDescription = '',
    excludeFromMd = ['api_reference/'],
    excludeFromIndex = ['devel/'],
    separateIndexes = [],
  } = options;

  return {
    name: 'llms-txt',

    async postBuild({outDir, siteConfig}) {
      const baseUrl = siteConfig.baseUrl || '/';
      const siteDir = _context.siteDir;

      // Extract docs version config from preset
      const presetConfig = siteConfig.presets?.[0]?.[1] || {};
      const docsVersions = presetConfig.docs?.versions || {};

      // Build version routing table
      const versionRoutes = buildVersionRoutes(siteDir, docsVersions);
      console.log(
        `[llms-txt] Version routes: ${versionRoutes.map((r) => `"${r.prefix || '/'}" → ${path.basename(r.sourceDir)}`).join(', ')}`
      );

      // Pre-build slug maps for each source dir
      const slugMaps = {};
      for (const route of versionRoutes) {
        if (fs.existsSync(route.sourceDir)) {
          slugMaps[route.sourceDir] = buildSlugMap(route.sourceDir);
        }
      }

      // Step 1: Discover pages from HTML build output
      const htmlFiles = walkFiles(outDir, (name) => name.endsWith('.html'));
      const pages = [];

      for (const htmlFile of htmlFiles) {
        const rel = path.relative(outDir, htmlFile);
        const relPosix = rel.split(path.sep).join('/');

        // Skip non-doc files
        if (
          relPosix === '404.html' ||
          relPosix === 'search.html' ||
          relPosix.startsWith('assets/') ||
          relPosix.startsWith('search/')
        ) {
          continue;
        }

        // Skip excluded patterns (e.g., api_reference/)
        if (excludeFromMd.some((p) => relPosix.includes(p))) {
          continue;
        }

        // Match to a version route (longest prefix wins)
        const route = versionRoutes.find((r) =>
          r.prefix === '' ? true : relPosix.startsWith(r.prefix)
        );
        if (!route || !fs.existsSync(route.sourceDir)) {
          continue;
        }

        // Strip version prefix to get the inner doc path
        const innerRel = route.prefix
          ? relPosix.slice(route.prefix.length)
          : relPosix;

        // Step 2: Map HTML path to source .md file
        const basename = path.basename(innerRel);

        // Skip underscore-prefixed partials (MDX fragments imported by other pages)
        if (basename.startsWith('_')) {
          continue;
        }

        let sourceFile = null;

        if (basename === 'index.html') {
          const dirPart = path.dirname(innerRel);
          const candidate = path.join(route.sourceDir, dirPart, 'index.md');
          if (fs.existsSync(candidate)) {
            sourceFile = candidate;
          }
        } else {
          const stem = innerRel.replace(/\.html$/, '');
          // Try .md, then .mdx, then slug map
          const candidateMd = path.join(route.sourceDir, stem + '.md');
          const candidateMdx = path.join(route.sourceDir, stem + '.mdx');
          const slugMap = slugMaps[route.sourceDir] || {};

          if (fs.existsSync(candidateMd)) {
            sourceFile = candidateMd;
          } else if (fs.existsSync(candidateMdx)) {
            sourceFile = candidateMdx;
          } else if (slugMap[stem]) {
            sourceFile = slugMap[stem];
          }
        }

        if (!sourceFile) {
          continue; // generated-index or auto-generated page, no source
        }

        pages.push({
          htmlRel: relPosix,
          mdRel: relPosix.replace(/\.html$/, '.md'),
          sourceFile,
          isMaster: route.isMaster,
        });
      }

      console.log(`[llms-txt] Found ${pages.length} pages with source .md files`);

      // Step 3: Copy source .md files with cleanup
      let copiedCount = 0;
      for (const page of pages) {
        const destFile = path.join(outDir, page.mdRel);
        const destDir = path.dirname(destFile);
        if (!fs.existsSync(destDir)) {
          fs.mkdirSync(destDir, {recursive: true});
        }
        const content = fs.readFileSync(page.sourceFile, 'utf8');
        const cleaned = cleanMarkdown(content);
        fs.writeFileSync(destFile, cleaned);
        copiedCount++;
      }
      console.log(`[llms-txt] Copied ${copiedCount} .md files to build output`);

      // Step 4: Generate llms.txt (master version only)
      const masterPages = pages.filter(
        (p) => p.isMaster && !excludeFromIndex.some((pat) => p.mdRel.includes(pat))
      );

      /**
       * Build llms.txt content from a set of pages.
       * @param {string} title - The title for the llms.txt header
       * @param {string} description - Description for the blockquote
       * @param {Array} pageList - Pages to include
       * @param {string} stripPrefix - Prefix to strip from mdRel for grouping
       */
      function buildLlmsTxt(title, description, pageList, stripPrefix = '') {
        // Group by first path segment (after stripping prefix)
        const groups = {};
        const topLevel = [];
        for (const page of pageList) {
          const content = fs.readFileSync(page.sourceFile, 'utf8');
          const fm = readFrontmatter(content);
          const pageTitle = fm.title || path.basename(page.mdRel, '.md');
          const pageDesc = fm.description || '';

          const groupPath = stripPrefix
            ? page.mdRel.slice(stripPrefix.length)
            : page.mdRel;
          const parts = groupPath.split('/');
          if (parts.length === 1) {
            topLevel.push({mdRel: page.mdRel, title: pageTitle, description: pageDesc});
          } else {
            const group = parts[0];
            if (!groups[group]) groups[group] = [];
            groups[group].push({mdRel: page.mdRel, title: pageTitle, description: pageDesc});
          }
        }

        let txt = `# ${title}\n\n`;
        if (description) {
          const descLines = description.split('\n').map((l) => `> ${l}`);
          txt += descLines.join('\n') + '\n\n';
        }

        if (topLevel.length > 0) {
          for (const p of topLevel.sort((a, b) => a.title.localeCompare(b.title))) {
            const desc = p.description ? `: ${p.description}` : '';
            txt += `- [${p.title}](${baseUrl}${p.mdRel})${desc}\n`;
          }
          txt += '\n';
        }

        const sortedGroups = Object.keys(groups).sort();
        for (const group of sortedGroups) {
          txt += `## ${group}\n`;
          const sortedPages = groups[group].sort((a, b) => a.title.localeCompare(b.title));
          for (const p of sortedPages) {
            const desc = p.description ? `: ${p.description}` : '';
            txt += `- [${p.title}](${baseUrl}${p.mdRel})${desc}\n`;
          }
          txt += '\n';
        }

        return {txt, groups: sortedGroups, count: pageList.length};
      }

      // Generate separate llms.txt files for configured prefixes
      let remainingPages = masterPages;
      for (const sep of separateIndexes) {
        const prefix = sep.prefix;
        const matching = remainingPages.filter((p) => p.mdRel.startsWith(prefix));
        remainingPages = remainingPages.filter((p) => !p.mdRel.startsWith(prefix));

        if (matching.length === 0) continue;

        const {txt, groups, count} = buildLlmsTxt(sep.title, sep.description, matching, prefix);
        const sepPath = path.join(outDir, prefix, 'llms.txt');
        const sepDir = path.dirname(sepPath);
        if (!fs.existsSync(sepDir)) {
          fs.mkdirSync(sepDir, {recursive: true});
        }
        fs.writeFileSync(sepPath, txt);
        console.log(
          `[llms-txt] Generated ${prefix}llms.txt with ${count} entries in ${groups.length} groups`
        );
      }

      // Generate main llms.txt from remaining pages
      const {txt: mainTxt, groups: mainGroups, count: mainCount} = buildLlmsTxt(
        siteTitle, siteDescription, remainingPages
      );
      fs.writeFileSync(path.join(outDir, 'llms.txt'), mainTxt);
      console.log(
        `[llms-txt] Generated llms.txt with ${mainCount} entries in ${mainGroups.length} groups`
      );
    },
  };
};
