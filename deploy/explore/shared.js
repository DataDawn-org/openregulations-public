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

// --- Datasette query helper ---

async function query(sql, retries = 1) {
    const url = `${API}.json?sql=${encodeURIComponent(sql)}&_shape=objects`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000);
    try {
        const resp = await fetch(url, { signal: controller.signal });
        clearTimeout(timeoutId);
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
