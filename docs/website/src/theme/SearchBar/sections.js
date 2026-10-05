/**
 * Search tuning, kept in code. Each doc page gets tagged at build time with:
 * - `section`: a coarse bucket users can toggle in the search UI
 * - `category`: the top-level sidebar category (e.g. "Sources", "Destinations")
 * - a ranking `weight` multiplier for its content (Pagefind default is 1)
 *
 * Rules are matched in order against the doc id (path under docs_processed).
 */
const SECTION_RULES = [
  {prefix: 'api_reference/', section: 'API reference', weight: 0.5},
  {prefix: 'release-notes/', section: 'Release notes', weight: 0.3},
  {prefix: 'hub/', section: 'dltHub', weight: 1},
  {prefix: 'examples/', section: 'Cookbook', weight: 0.8},
  {prefix: 'walkthroughs/', section: 'Cookbook', weight: 0.8},
  {prefix: 'tutorial/', section: 'Education', weight: 0.8},
];

const DEFAULT_SECTION = {section: 'dlt', weight: 1};

// Order of the section toggles in the search UI
export const SECTION_ORDER = [
  'dlt',
  'dltHub',
  'Cookbook',
  'Education',
  'API reference',
  'Release notes',
];

export function getSearchSection(docId) {
  return SECTION_RULES.find((r) => docId.startsWith(r.prefix)) ?? DEFAULT_SECTION;
}
