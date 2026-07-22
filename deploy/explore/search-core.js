/* search-core.js — shared search substrate (Order 0, search-surface plan 2026-07-22;
 * audit: working-docs/search_audit_2026-07-22.md).
 *
 * CANONICAL COPY: datadawn-website/search-core.js
 * VERBATIM COPIES (keep byte-identical; tools/search_core_sync_check.sh diffs them):
 *   openregs/deploy/explore/search-core.js
 *   990project/explore/search-core.js
 * Python mirror (MCP tools): datadawn-mcp/server.py "search substrate" block.
 *
 * Two jobs, nothing else:
 *   sanitizeFTS(q, opts) — one deliberate FTS5 policy: strip operator/syntax chars, quote
 *     every token (implicit AND, any order). opts.prefix appends a per-token '*' — for
 *     name/entity boxes ONLY, never full-text tabs. Returns the MATCH expression WITHOUT
 *     SQL-escaping; callers still sqlEsc/bind it.
 *   detect(q) — identifier-shape candidates. Returns an ARRAY because shapes overlap (a
 *     9-digit number is both an EIN and a CIK candidate): each caller filters to the types
 *     its box accepts. Types: ein, bill, docket, frdoc, frcite, cik, ticker.
 */
(function (root) {
    'use strict';

    function sanitizeFTS(q, opts) {
        var prefix = !!(opts && opts.prefix);
        var s = String(q == null ? '' : q).replace(/["*(){}[\]^~:]/g, ' ');
        var toks = s.trim().split(/\s+/).filter(Boolean);
        if (!toks.length) return '';
        return toks.map(function (t) { return '"' + t + '"' + (prefix ? '*' : ''); }).join(' ');
    }

    // Bill-number regex reused VERBATIM from legislation.html (the one surface that already
    // parsed human bill forms correctly) — do not re-invent (plan Order 0).
    var BILL_RE = /^(h\.?r\.?|s\.?|h\.?j\.?res\.?|s\.?j\.?res\.?|h\.?res\.?|s\.?res\.?|h\.?con\.?res\.?|s\.?con\.?res\.?)\s*(\d+)$/i;
    // Docket-ID shape shipped 2026-07-22 (index.html + regulation.html): 2+ leading letters,
    // hyphen/underscore-separated groups, must contain a digit.
    var DOCKET_RE = /^[A-Za-z]{2,}(?:[-_][A-Za-z0-9.]+)+$/;
    // FR document number: optional single-letter era prefix (E9-…, 05-…, 2025-12345).
    var FRDOC_RE = /^([A-Za-z]?\d{1,4})-(\d{3,6})$/;
    var FRCITE_RE = /^(\d{1,3})\s+fr\s+(\d{1,6})$/i;
    var URL_SEG_RE = /\/(docket|document)\/([A-Za-z0-9_.-]+)/i;

    function detect(q) {
        var raw = String(q == null ? '' : q).trim();
        var out = [];
        if (!raw) return out;

        // Pasted URL (ours or regulations.gov): extract the docket segment. A /document/
        // id keeps its trailing item counter stripped so it resolves to the parent docket.
        var urlM = raw.match(URL_SEG_RE);
        var cand = raw;
        var fromUrl = false;
        if (urlM) {
            cand = urlM[2];
            if (urlM[1].toLowerCase() === 'document') cand = cand.replace(/-\d+$/, '');
            fromUrl = true;
        }

        if (/\d/.test(cand) && DOCKET_RE.test(cand)) {
            out.push({ type: 'docket', value: cand.toUpperCase(), fromUrl: fromUrl });
        }

        var billM = raw.match(BILL_RE);
        if (billM) {
            out.push({
                type: 'bill',
                billType: billM[1].replace(/\./g, '').toUpperCase(),
                billNumber: parseInt(billM[2], 10)
            });
        }

        if (/^[\d\s-]+$/.test(raw)) {
            var digits = raw.replace(/\D/g, '');
            if (digits.length === 9 && raw.length <= 11) out.push({ type: 'ein', value: digits });
            if (digits.length >= 1 && digits.length <= 10 && /^[\d]+$/.test(raw)) {
                out.push({ type: 'cik', value: digits });
            }
        }

        var frdocM = raw.match(FRDOC_RE);
        if (frdocM) out.push({ type: 'frdoc', value: raw.toUpperCase() });

        var frciteM = raw.match(FRCITE_RE);
        if (frciteM) {
            out.push({ type: 'frcite', volume: parseInt(frciteM[1], 10), page: parseInt(frciteM[2], 10) });
        }

        if (/^[A-Za-z]{1,5}$/.test(raw)) out.push({ type: 'ticker', value: raw.toUpperCase() });

        return out;
    }

    // Order-3 consumer (member finders): strip a leading honorific so "Sen. Cruz" matches.
    function stripHonorific(q) {
        return String(q == null ? '' : q).replace(/^(senator|sen|representative|rep|congressman|congresswoman|dr|mr|ms|mrs)\.?\s+/i, '');
    }

    var api = { sanitizeFTS: sanitizeFTS, detect: detect, stripHonorific: stripHonorific };
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
    else root.SearchCore = api;
})(typeof self !== 'undefined' ? self : this);
