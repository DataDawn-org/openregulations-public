// DataDawn OpenRegs — Shared Utilities
// All explore pages include this file. Edit here, not in individual pages.
// Each page defines `const API = '...'` in its inline script block.

// --- HTML / SQL escaping ---

function esc(str) {
    if (str == null) return '';
    const d = document.createElement('div');
    d.textContent = String(str);
    return d.innerHTML;
}


// ── COMMENT TEXT RENDERING — escape everything, THEN allowlist (2026-09-19) ─────────────
// The API channel stores regulations.gov's PUBLISHED HTML verbatim (confirmed 2026-09-19
// against GET /v4/comments/{id}), so comment_text carries <br/> and HTML entities. esc()
// escaped them and the browser un-escaped them straight back to VISIBLE CHARACTERS:
// readers saw a literal "<br/>" on 172,170 of 292,658 served texts (58.8%) and literal
// entities on 130,778 (44.7%), while `white-space: pre-wrap` did nothing at all because the
// column holds ZERO newlines. Record: queue_inbox/2026-09-19_served_comment_text_displays
// _literal_html_markup.md.
//
// THE ORDER IS THE WHOLE FIX. esc() EVERYTHING FIRST, exactly as before, and only then
// convert a CLOSED ALLOWLIST of already-escaped markers. Nothing is un-escaped except the
// forms named below, so anything not on the list — including a <script> a commenter submits
// tomorrow — stays inert text. NEVER drop esc(); NEVER assign third-party comment text to
// innerHTML unescaped. This is public submitted content.
//
// ALLOWLIST SIZED FROM THE CORPUS, 2026-09-19. The only tag forms that exist in all 292,658
// served texts are <br/> (172,170 rows) and the EMPTY padding-left span (18,558). <br>,
// <br />, <p>, <div, <a, <strong and <script are all ZERO. <br> is included anyway: it costs
// nothing and guards future data.
//
// THE SPAN BECOMES A TAB. It is an empty 30px indent spacer, and a tab is what the bulk
// channel's own decoder produced for it — so both channels render identically under
// pre-wrap, and the author's indent survives.
//
// ⚠ TWO ESCAPERS, TWO ESCAPED FORMS — this is why the quote class below is a group and not
// a literal. explore/shared.js's esc() is DOM-based (textContent -> innerHTML) and escapes
// only & < >, so the span's single quotes SURVIVE as '. datadawn-website/regs-shared.js's
// esc() is regex-based and turns ' into &#39;. One pattern has to match both, or the fix
// silently works on one surface and not the other.
var _COMMENT_ENT_OK = /&amp;(rsquo|lsquo|ldquo|rdquo|quot|apos|ndash|mdash|nbsp|amp|#39);/g;
var _COMMENT_SPAN = /&lt;span style=(?:&#39;|&apos;|&quot;|'|")padding-left:\s*30px(?:&#39;|&apos;|&quot;|'|")&gt;&lt;\/span&gt;/gi;
var _COMMENT_BR = /&lt;br\s*\/?&gt;/gi;

function escComment(str) {
    return esc(str)
        .replace(_COMMENT_SPAN, '\t')
        .replace(_COMMENT_BR, '\n')
        .replace(_COMMENT_ENT_OK, '&$1;');
}

function sqlEsc(s) { return s ? s.replace(/'/g, "''") : ''; }

// Escape a value for embedding inside a single-quoted JS string inside a
// double-quoted HTML attribute, e.g.: `<div onclick="openClient('${jsAttr(name)}')">`.
// Handles both the JS-string escape (\ and ') and the HTML-attribute escape (&, ", <, >).
function jsAttr(s) {
    return String(s == null ? '' : s)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'")
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

// --- Rate-limit retry toast ---
// Shown by query() while it waits out a 429 retry, so the user sees activity
// instead of a frozen spinner. Ref-counted because explore pages fire many
// queries in parallel (Promise.all) that can all be retrying at once.
let _retryToastDepth = 0;
function showRetryToast(secs) {
    _retryToastDepth++;
    let el = document.getElementById('dd-retry-toast');
    if (!el) {
        el = document.createElement('div');
        el.id = 'dd-retry-toast';
        el.style.cssText = 'position:fixed;left:50%;bottom:1.5rem;transform:translateX(-50%);' +
            'z-index:10000;background:#f59e0b;color:#0a0e1a;font-family:"IBM Plex Mono",monospace;' +
            'font-size:0.8rem;padding:0.6rem 1.1rem;border-radius:8px;' +
            'box-shadow:0 6px 24px rgba(0,0,0,0.35);transition:opacity 0.25s;';
        document.body.appendChild(el);
    }
    el.textContent = 'Rate limit reached — retrying in ' + secs + 's…';
    el.style.opacity = '1';
}
function hideRetryToast() {
    _retryToastDepth = Math.max(0, _retryToastDepth - 1);
    if (_retryToastDepth === 0) {
        const el = document.getElementById('dd-retry-toast');
        if (el) el.style.opacity = '0';
    }
}

// --- Datasette query helper ---

async function query(sql, retries = 2) {
    const url = `${API}.json?sql=${encodeURIComponent(sql)}&_shape=objects`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000);
    try {
        const resp = await fetch(url, { signal: controller.signal });
        clearTimeout(timeoutId);
        if (resp.status === 429 && retries > 0) {
            // Rate limited. The limit is a 60s *sliding* window, so a token frees
            // up within a few seconds — cap the wait at 5s instead of sleeping the
            // server's pessimistic Retry-After (it pins 60), which would freeze an
            // interactive page for a full minute. Show a toast, not a silent spinner.
            const retryAfter = Math.min(parseFloat(resp.headers.get('Retry-After')) || 1.5, 5);
            showRetryToast(Math.ceil(retryAfter));
            await new Promise(r => setTimeout(r, retryAfter * 1000));
            try { return await query(sql, retries - 1); }
            finally { hideRetryToast(); }
        }
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.error || `HTTP ${resp.status}`);
        }
        const data = await resp.json();
        return data.rows || [];
    } catch (e) {
        clearTimeout(timeoutId);
        // A query cut by the time limit is NOT re-sent (relay datadawn pink #1 item 4; search-surface inbox
        // 2026-10-03, fix 2). Page SQL from non-exempt clients stops at a forced 2 s (decisions_log §286 (a)), so the
        // same query stops again, and each retry holds one of the host's two page-SQL slots (§288) for another 2 s
        // while the page's other queries, and other readers', wait. The AbortError retry stays: it covers this
        // helper's own 15 s client-side abort.
        if (retries > 0 && e.name === 'AbortError') {
            console.log('Query aborted after 15 s, retrying...');
            return query(sql, retries - 1);
        }
        throw e;
    }
}

// ── A.6 bulk-text availability (decisions_log §129/§130, 2026-09-20) ──────────────────────
// The served DB may or may not carry the S4 bulk merge. Comment-text reads ask ONCE per page
// load whether `comment_bodies` and `comment_texts` exist and fall back to the
// comment_details-only shape when they do not — so a page deploy landing before the merged
// DB (or a DB rollback landing after it) degrades to API-only coverage instead of erroring.
// Mirror in datadawn-website/regs-shared.js (change one, change both).
const TEXT_SOURCE_API = 'regulations.gov API';
const TEXT_SOURCE_BULK = 'regulations.gov bulk export (markup removed)';
let _bulkTextProbe = null;
function bulkTextAvailable() {
    if (!_bulkTextProbe) {
        _bulkTextProbe = queryCount(
            `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('comment_bodies','comment_texts')`
        ).then(n => n === 2).catch(() => false);
    }
    return _bulkTextProbe;
}
// ── §128/§144 suppression marker (item 1, 2026-09-22) ────────────────────────────────────
// comments.suppression_reason says WHY a served comment carries no text. Probed once per page
// load for the same reason bulkTextAvailable is: deploy.sh ships explore/ in full and --db-only
// modes, so the page can land BEFORE the DB that has the column, and a page that errors on a
// missing column is worse than one that degrades to the older explanation.
// ⚠ THE FALLBACK IS NOT COSMETIC. On a DB without the column, `withdrawn = 1` is the best
// available test and it is INCOMPLETE: it misses the reason-set/boolean-0 disagreement rows,
// which are suppressed while carrying withdrawn = 0. Those rows render with no explanation on an
// old DB — the 811c065 defect — and that is exactly what the column exists to end. The fallback
// buys a deploy ordering, not a correct answer.
const SUPPRESSION_TEXT = {
    withdrawn: 'This comment is marked withdrawn on Regulations.gov.',
    // §144's approved wording, VERBATIM. It is a statement about the SOURCE's publication state,
    // not about the comment, the commenter, or any cause — we do not know why a comment stopped
    // being published and must not imply one. Do not reword: decisions_log §145 requires the dump's
    // grain note to reuse this string exactly, and a second wording here would fork them.
    not_posted: 'No longer posted on regulations.gov',
};
// ── Operator suppression notes (maintainer ruling 2026-09-25, decisions_log §167 punch-list 4) ────
// A reason of the form 'operator:<class>:<YYYY-MM-DD>' means DATADAWN withheld the text, not the
// source. Every note must name DataDawn as the actor, the date, the class, and that the filing
// remains on the public docket at the source, so it can never be read as the source-withdrawal
// arm above. ⚠ DRAFT WORDING — maintainer edits these strings; 50_generate_dumps.py's grain note
// carries the same sentences per §145 and must be changed in the same commit (this file is the
// source of truth, that one is the mirror).
const OPERATOR_NOTE_DRAFT = {
    privacy: d => `Text withheld by DataDawn on ${d} (privacy: personal information of a private individual beyond what the source redacted). The comment itself remains on the public docket at regulations.gov. How to request a correction: https://datadawn.org/methodology#independence`,
    legal:   d => `Text withheld by DataDawn on ${d} in response to a legal request. The comment itself remains on the public docket at regulations.gov. How to request a correction: https://datadawn.org/methodology#independence`,
    review:  d => `Text temporarily withheld by DataDawn on ${d} while a report about it is reviewed. The comment itself remains on the public docket at regulations.gov. How to request a correction: https://datadawn.org/methodology#independence`,
};
// suppressionNote(reason): the sentence for a suppression_reason value, or null when the value
// is unknown (an unknown value renders NO explanation rather than a wrong one — and the build
// refuses any value outside the vocabulary, so null here means a page/DB skew, not data).
function suppressionNote(reason) {
    if (!reason) return null;
    if (SUPPRESSION_TEXT[reason]) return SUPPRESSION_TEXT[reason];
    const m = /^operator:(privacy|legal|review):(\d{4}-\d{2}-\d{2})$/.exec(reason);
    if (m && OPERATOR_NOTE_DRAFT[m[1]]) return OPERATOR_NOTE_DRAFT[m[1]](m[2]);
    return null;
}
let _suppProbe = null;
function suppressionReasonAvailable() {
    if (!_suppProbe) {
        _suppProbe = queryCount(
            `SELECT COUNT(*) FROM pragma_table_info('comments') WHERE name='suppression_reason'`
        ).then(n => n === 1).catch(() => false);
    }
    return _suppProbe;
}
// suppressionShape(hasCol): the SELECT expression and the reader for either DB state.
function suppressionShape(hasCol) {
    return {
        col: hasCol ? `c.suppression_reason` : `NULL`,
        // FIELD FIRST, then the fallback. A row suppressed with withdrawn = 0 is invisible to the
        // fallback, so the field has to win wherever it exists.
        reasonOf: row => row.suppression_reason || (Number(row.withdrawn) ? 'withdrawn' : null),
    };
}

// textShape(bulk): the join clause plus column expressions for comment text + submitter metadata in
// either DB state. API text is preferred where both channels hold it; the channel is reported
// per row as text_source (§129: two values, computed at query time, never stored).
// attachment_count / attachment_urls stay API-only: bulk's count is a DIFFERENT definition
// (format count, see comment_bodies.attachment_format_count) and must not share the name.
function textShape(bulk) {
    if (!bulk) {
        return {
            join: `LEFT JOIN comment_details cd ON cd.id = c.id`,
            present: `cd.id IS NOT NULL`,
            text: `cd.comment_text`,
            src: `'${TEXT_SOURCE_API}'`,
            col: name => `cd.${name}`,
        };
    }
    const text = `COALESCE(NULLIF(cd.comment_text, ''), ct.comment_text)`;
    return {
        join: `LEFT JOIN comment_details cd ON cd.id = c.id LEFT JOIN comment_bodies cb ON cb.id = c.id LEFT JOIN comment_texts ct ON ct.body_sha256 = cb.body_sha256`,
        present: `(cd.id IS NOT NULL OR cb.id IS NOT NULL)`,
        text,
        src: `CASE WHEN NULLIF(cd.comment_text, '') IS NOT NULL THEN '${TEXT_SOURCE_API}' WHEN ct.comment_text IS NOT NULL THEN '${TEXT_SOURCE_BULK}' END`,
        col: name => `COALESCE(cd.${name}, cb.${name})`,
    };
}

// queryCount(sql): Run a SELECT COUNT(*) query and return the integer count.
// Used for displaying true total result counts in search pages — never display
// "30+" when you can display the actual number. Returns null on error so the
// caller can fall back to displaying the page-size count.
//
// The SQL must return a single row with a single column (the count). Both
// "SELECT COUNT(*) FROM ..." and "SELECT COUNT(*) AS cnt FROM ..." work.
async function queryCount(sql) {
    try {
        const rows = await query(sql);
        if (!rows || rows.length === 0) return null;
        const row = rows[0];
        // First value in the row, regardless of column name
        const v = Object.values(row)[0];
        return typeof v === 'number' ? v : (v != null ? Number(v) : null);
    } catch (e) {
        console.warn('queryCount failed:', e.message);
        return null;
    }
}

// formatResultCount(totalCount, displayLength, hasMore): Build the display
// label for a search result count. Prefers the true total count from a
// COUNT(*) query; falls back to "${displayLength}+" if the count is unavailable.
// Used by all explore pages to keep result count display consistent.
function formatResultCount(totalCount, displayLength, hasMore) {
    if (totalCount != null) return Number(totalCount).toLocaleString();
    return `${displayLength}${hasMore ? '+' : ''}`;
}

// --- URL state management ---

function getParam(k) { return new URLSearchParams(location.search).get(k); }

function setParams(params) {
    const u = new URLSearchParams(location.search);
    for (const [k, v] of Object.entries(params)) {
        if (v === null || v === undefined || v === '') u.delete(k);
        else u.set(k, v);
    }
    const str = u.toString();
    history.replaceState(null, '', str ? '?' + str : location.pathname);
}

// --- Text helpers ---

function truncate(str, len) {
    if (!str) return '';
    return str.length > len ? str.slice(0, len) + '...' : str;
}

function normalizeName(name) {
    return (name || '').replace(/[.,;:!?]/g, '').replace(/\s+/g, ' ').trim().toUpperCase();
}

function ord(n) { const s = ["th","st","nd","rd"], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); }

// --- Number formatting ---

// Compact number (no $): 1234 -> "1K", 1234567 -> "1.2M"
function fmt(n) {
    if (n == null) return '\u2014';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(0) + 'K';
    return Number(n).toLocaleString();
}

// Compact count: like fmt but uses .toFixed(1) for thousands
function fmtCount(n) {
    if (n == null) return '\u2014';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return Number(n).toLocaleString();
}

// Compact dollar amount: 1234 -> "$1K", 1234567 -> "$1.2M"
function fmtNum(n) {
    if (n == null) return '\u2014';
    if (n >= 1e9) return '$' + (n / 1e9).toFixed(1) + 'B';
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(0) + 'K';
    return '$' + Number(n).toLocaleString();
}

// Full-precision dollar amount
function fmtMoney(n) {
    if (n == null || n === 0) return '\u2014';
    return '$' + Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

// Full-precision number, null -> em dash
function formatNumber(n) {
    if (n == null) return '\u2014';
    return Number(n).toLocaleString();
}

// Compact dollar with NaN guard, trillions, zero handling
function formatMoney(val) {
    if (val == null || val === '' || val === 0) return '\u2014';
    const num = Number(val);
    if (isNaN(num)) return String(val);
    const abs = Math.abs(num);
    if (abs >= 1e12) return '$' + (num / 1e12).toFixed(1) + 'T';
    if (abs >= 1e9) return '$' + (num / 1e9).toFixed(1) + 'B';
    if (abs >= 1e6) return '$' + (num / 1e6).toFixed(1) + 'M';
    if (abs >= 1e3) return '$' + (num / 1e3).toFixed(0) + 'K';
    return '$' + num.toLocaleString();
}

// --- Example search chip handler ---

function exSearch(term) {
    const el = document.getElementById('searchInput');
    if (el) {
        el.value = term;
        if (typeof doSearch === 'function') doSearch();
    }
}
