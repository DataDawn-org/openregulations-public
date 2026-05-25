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
        if (retries > 0 && (e.name === 'AbortError' || e.message.includes('SQL query took too long'))) {
            console.log('Query timed out, retrying (cache should be warm now)...');
            return query(sql, retries - 1);
        }
        throw e;
    }
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
