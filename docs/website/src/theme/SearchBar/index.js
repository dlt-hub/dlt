/**
 * Pagefind search with section toggles. The index is produced by `pagefind` after
 * `docusaurus build` (`npm run search-index`, see pagefind.yml) and its JS API is
 * loaded lazily from `<baseUrl>pagefind/pagefind.js`.
 *
 * Pages are tagged with a `section` filter (see ./sections.js). Toggles are OR-ed:
 * results come from any enabled section.
 */
import React, {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import clsx from 'clsx';
import {useHistory} from '@docusaurus/router';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {SECTION_ORDER} from './sections';
import styles from './styles.module.css';

// Pagefind ranking knobs (defaults shown), see https://pagefind.app/docs/ranking/
const RANKING = {
  termFrequency: 1.0,
  pageLength: 0.75,
  termSaturation: 1.4,
  termSimilarity: 1.0,
};
const PAGE_SIZE = 10;
const MAX_SUB_RESULTS = 3;
const STORAGE_KEY = 'dlt-search-disabled-sections';

// Docusaurus is built with trailingSlash: false -> pages are `foo.html`, routes are `foo`
const stripHtml = (url) => url.replace(/\.html(?=$|#|\?)/, '');

async function loadPagefind(bundleUrl, baseUrl) {
  // A missing file falls back to index.html in dev, check we get the real index
  const res = await fetch(`${bundleUrl}pagefind-entry.json`);
  if (!res.ok || !res.headers.get('content-type')?.includes('json')) {
    throw new Error('Pagefind index not found');
  }
  const pagefind = await import(/* webpackIgnore: true */ `${bundleUrl}pagefind.js`);
  await pagefind.options({baseUrl, ranking: RANKING});
  await pagefind.init();
  return pagefind;
}

function readDisabledSections() {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    return new Set(stored ? JSON.parse(stored) : []);
  } catch {
    return new Set();
  }
}

function sortSections(names) {
  const rank = (s) => (SECTION_ORDER.includes(s) ? SECTION_ORDER.indexOf(s) : SECTION_ORDER.length);
  return [...names].sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));
}

function Result({result, onNavigate}) {
  const section = result.filters?.section?.[0];
  const subResults = (result.sub_results ?? [])
    .filter((sub) => sub.url.includes('#'))
    .slice(0, MAX_SUB_RESULTS);
  return (
    <li className={styles.result}>
      <a href={result.url} onClick={onNavigate} className={styles.resultTitle}>
        {result.meta?.title}
      </a>
      {section && <span className={styles.badge}>{section}</span>}
      {/* excerpts are escaped by Pagefind, only <mark> is added */}
      <p className={styles.excerpt} dangerouslySetInnerHTML={{__html: result.excerpt}} />
      {subResults.length > 0 && (
        <ul className={styles.subResults}>
          {subResults.map((sub) => (
            <li key={sub.url}>
              <a href={sub.url} onClick={onNavigate}>
                {sub.title}
              </a>
              <p className={styles.excerpt} dangerouslySetInnerHTML={{__html: sub.excerpt}} />
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

function SearchModal({onClose, bundleUrl, baseUrl}) {
  const history = useHistory();
  const inputRef = useRef(null);
  const pagefindRef = useRef(null);
  const [error, setError] = useState(false);
  const [query, setQuery] = useState('');
  const [sections, setSections] = useState([]); // all sections in the index
  const [disabled, setDisabled] = useState(readDisabledSections);
  const [search, setSearch] = useState(null); // raw Pagefind search response
  const [results, setResults] = useState([]); // loaded result data
  const [shown, setShown] = useState(PAGE_SIZE);

  useEffect(() => {
    inputRef.current?.focus();
    loadPagefind(bundleUrl, baseUrl)
      .then(async (pagefind) => {
        pagefindRef.current = pagefind;
        const filters = await pagefind.filters();
        setSections(sortSections(Object.keys(filters.section ?? {})));
      })
      .catch(() => setError(true));
  }, [bundleUrl, baseUrl]);

  const enabled = useMemo(() => sections.filter((s) => !disabled.has(s)), [sections, disabled]);

  // Run the search when query or toggles change
  useEffect(() => {
    const pagefind = pagefindRef.current;
    if (!pagefind || !query.trim() || (sections.length > 0 && enabled.length === 0)) {
      setSearch(null);
      return;
    }
    // an index built without section tags has no sections: don't filter
    const filters = sections.length > 0 ? {section: {any: enabled}} : {};
    let cancelled = false;
    pagefind
      .debouncedSearch(query, {filters}, 150)
      .then((response) => {
        // null when superseded by a newer search
        if (response && !cancelled) {
          setSearch(response);
          setShown(PAGE_SIZE);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [query, enabled, sections]);

  // Load data of the visible results only
  useEffect(() => {
    if (!search) {
      setResults([]);
      return;
    }
    let cancelled = false;
    Promise.all(search.results.slice(0, shown).map((r) => r.data())).then((data) => {
      if (!cancelled) {
        setResults(
          data.map((d) => ({
            ...d,
            url: stripHtml(d.url),
            sub_results: d.sub_results?.map((s) => ({...s, url: stripHtml(s.url)})),
          })),
        );
      }
    });
    return () => {
      cancelled = true;
    };
  }, [search, shown]);

  const toggleSection = (section, only) => {
    const next = only
      ? new Set(sections.filter((s) => s !== section))
      : new Set(disabled);
    if (!only) {
      next.has(section) ? next.delete(section) : next.add(section);
    }
    setDisabled(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify([...next]));
    } catch {}
  };

  // Navigate client-side instead of full page reloads
  const onNavigate = useCallback(
    (e) => {
      const link = e.currentTarget;
      if (e.metaKey || e.ctrlKey || e.shiftKey) {
        return;
      }
      e.preventDefault();
      onClose();
      history.push(link.pathname + link.hash);
    },
    [history, onClose],
  );

  const onKeyDown = (e) => {
    if (e.key === 'Enter' && results[0]) {
      onClose();
      history.push(results[0].url);
    }
  };

  // counts of matches per section for the current query, ignoring toggles
  const counts = search?.totalFilters?.section ?? {};

  return (
    <div className={styles.modal} role="dialog" aria-label="Search">
      <input
        ref={inputRef}
        className={styles.input}
        type="search"
        placeholder="Search docs"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={onKeyDown}
      />
      {sections.length > 0 && (
        <div className={styles.toggles} title="Alt+click: show only this section">
          {sections.map((section) => (
            <button
              key={section}
              type="button"
              aria-pressed={!disabled.has(section)}
              className={clsx(styles.toggle, !disabled.has(section) && styles.toggleOn)}
              onClick={(e) => toggleSection(section, e.altKey)}
            >
              {section}
              {search && <span className={styles.count}>{counts[section] ?? 0}</span>}
            </button>
          ))}
        </div>
      )}
      {error && (
        <p>
          Search index not found. Restart the dev server with <code>make start</code> (in{' '}
          <code>docs/</code>), it builds the index if missing.
        </p>
      )}
      {query.trim() && sections.length > 0 && enabled.length === 0 && (
        <p className={styles.summary}>All sections are hidden, enable one above.</p>
      )}
      {search && (
        <p className={styles.summary}>
          {search.results.length} results
          {search.unfilteredResultCount > search.results.length &&
            ` (${search.unfilteredResultCount - search.results.length} more in hidden sections)`}
        </p>
      )}
      <ul className={styles.results}>
        {results.map((result) => (
          <Result key={result.url} result={result} onNavigate={onNavigate} />
        ))}
      </ul>
      {search && shown < search.results.length && (
        <button type="button" className={styles.more} onClick={() => setShown(shown + PAGE_SIZE)}>
          Load more results
        </button>
      )}
    </div>
  );
}

export default function SearchBar() {
  const [open, setOpen] = useState(false);
  const baseUrl = useBaseUrl('/');
  const bundleUrl = useBaseUrl('/pagefind/');
  const close = useCallback(() => setOpen(false), []);

  // Cmd/Ctrl+K to open, Escape to close
  useEffect(() => {
    const onKeyDown = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        setOpen(true);
      } else if (e.key === 'Escape') {
        setOpen(false);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  return (
    <>
      <button type="button" className={styles.trigger} onClick={() => setOpen(true)}>
        <span>Search</span>
        <kbd className={styles.kbd}>⌘K</kbd>
      </button>
      {open && (
        // biome-ignore lint/a11y/useKeyWithClickEvents: Escape closes the modal
        <div className={styles.backdrop} onClick={(e) => e.target === e.currentTarget && close()}>
          <SearchModal onClose={close} bundleUrl={bundleUrl} baseUrl={baseUrl} />
        </div>
      )}
    </>
  );
}
