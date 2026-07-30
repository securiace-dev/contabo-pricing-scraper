/**
 * Contabo Pricing — admin UI behaviour.
 *
 * Plain vanilla ES2017+, no jQuery. Wires up:
 *   • filter pills + search + sort + bulk select on the profiles & sync-history tables
 *   • modal + drawer open/close
 *   • live quote preview (AJAX → /ajax-quote)
 *   • FX rate preview (AJAX → /ajax-fx, polled hourly)
 *   • Test API connection button (AJAX → /ajax-meta-probe)
 *   • Sparkline rendering (AJAX → /ajax-profile-versions)
 *   • Profile drawer hydration (AJAX → /ajax-profile)
 *   • Keyboard shortcuts: /, n, r, Esc
 *   • Toast feedback for AJAX actions
 *
 * Templates expose hooks via data-cb-* attributes — see docs/UI_ARCHITECTURE.md.
 *
 * Idempotent: guarded against double-initialisation if WHMCS evaluates the page
 * markup twice in one request (it sometimes does for sidebar widgets).
 */
(function () {
  'use strict';

  if (window.__cbPricingInit) return;
  window.__cbPricingInit = true;

  // ── small utils ────────────────────────────────────────────────────────────

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $$(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function debounce(fn, ms) {
    var t = null;
    return function () {
      var args = arguments, self = this;
      clearTimeout(t);
      t = setTimeout(function () { fn.apply(self, args); }, ms);
    };
  }

  function cbToken() {
    var input = document.querySelector('input[name="token"]');
    return input ? input.value : '';
  }

  function wireConfirmations() {
    $$('[data-cb-confirm]').forEach(function (element) {
      var eventName = element.tagName === 'FORM' ? 'submit' : 'click';
      element.addEventListener(eventName, function (event) {
        var message = element.getAttribute('data-cb-confirm') || 'Continue?';
        if (!window.confirm(message)) {
          event.preventDefault();
          event.stopPropagation();
        }
      });
    });
  }

  // Standalone AJAX endpoint — bypasses WHMCS admin chrome so the response
  // is pure JSON. The addonmodules.php route wrapped our JSON in HTML.
  // Host-absolute path so the admin slug (/shriram/, /admin/, etc.) is
  // irrelevant — module files live at /modules/addons/... from docroot.
  var AJAX_URL = '/modules/addons/contabo_pricing/ajax.php';

  function ajax(method, action, data) {
    // The endpoint accepts both 'quote' and 'ajax-quote'; we strip the prefix
    // so URLs stay clean.
    var act = String(action || '').replace(/^ajax-/, '');
    var url = AJAX_URL + '?action=' + encodeURIComponent(act);
    var opts = { method: method, headers: { 'Accept': 'application/json' }, credentials: 'same-origin' };
    if (method !== 'GET' && data) {
      var fd = new FormData();
      Object.keys(data).forEach(function (k) { fd.append(k, data[k]); });
      // CSRF token always included on mutators; harmless on reads.
      if (!fd.has('token')) fd.append('token', cbToken());
      opts.body = fd;
    } else if (method === 'GET' && data) {
      Object.keys(data).forEach(function (k) {
        url += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(data[k]);
      });
    }
    return fetch(url, opts).then(function (r) {
      return r.text().then(function (t) {
        try { return JSON.parse(t); }
        catch (_) {
          // Surface the first 200 chars of the body so the user sees WHAT
          // came back instead of just "invalid JSON". Helps debug 403/500s
          // returned as HTML error pages.
          return { error: 'Invalid JSON from server (HTTP ' + r.status + '): ' + t.slice(0, 200) };
        }
      });
    });
  }

  function cbToast(message, kind) {
    kind = kind || 'good';
    var el = document.createElement('div');
    el.className = 'cb-toast cb-pill ' + kind;
    el.textContent = String(message);
    document.body.appendChild(el);
    setTimeout(function () {
      if (el.parentNode) el.parentNode.removeChild(el);
    }, 3000);
  }

  // ── filter pills ───────────────────────────────────────────────────────────

  function applyFilter(table, value) {
    $$('tbody tr', table).forEach(function (tr) {
      var show = true;
      if (tr.hasAttribute('data-cb-profile-row') || tr.hasAttribute('data-cb-log-row')) {
        switch (value) {
          case 'all': show = true; break;
          case 'active':   show = tr.getAttribute('data-cb-active') === '1'; break;
          case 'inactive': show = tr.getAttribute('data-cb-active') === '0'; break;
          case 'drifted':  show = tr.getAttribute('data-cb-drifted') === '1'; break;
          case 'succeeded':
          case 'failed':
          case 'no-change':
            show = tr.getAttribute('data-cb-status') === value; break;
          default: show = true;
        }
      } else if (tr.hasAttribute('data-cb-status')) {
        // Generic status filter — used by the repricing dashboard rows.
        switch (value) {
          case 'all':     show = true; break;
          default:        show = tr.getAttribute('data-cb-status') === value;
        }
      }
      // Stash this filter's "would-show" flag so the cycle filter (below) can
      // AND with it without losing state.
      tr.setAttribute('data-cb-status-pass', show ? '1' : '0');
      applyCombinedFilters(tr);
    });
  }

  // Apply the cycle-pill filter (data-cb-filter-cycle="<cycle>") to all rows in
  // the table this button is attached to. "all" clears the cycle filter.
  function applyCycleFilter(table, value) {
    $$('tbody tr', table).forEach(function (tr) {
      var rowCycle = tr.getAttribute('data-cb-cycle') || '';
      var show = (value === 'all') || (rowCycle === value);
      tr.setAttribute('data-cb-cycle-pass', show ? '1' : '0');
      applyCombinedFilters(tr);
    });
  }

  // Combine the latest status + cycle filter results for a single row. Either
  // attribute defaults to "1" if never explicitly set, so a row stays visible
  // when only one of the two filters has been touched.
  function applyCombinedFilters(tr) {
    var statusPass = tr.getAttribute('data-cb-status-pass');
    var cyclePass  = tr.getAttribute('data-cb-cycle-pass');
    var ok = (statusPass === null || statusPass === '1')
          && (cyclePass  === null || cyclePass  === '1');
    tr.hidden = !ok;
  }

  function wireFilterPills() {
    $$('[data-cb-filter]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        var group = btn.parentNode;
        $$('[data-cb-filter]', group).forEach(function (b) { b.setAttribute('aria-pressed', 'false'); });
        btn.setAttribute('aria-pressed', 'true');
        var scope = btn.closest('.cb-card, section, body');
        var table = scope ? scope.querySelector('.cb-table') : null;
        if (!table) return;
        applyFilter(table, btn.getAttribute('data-cb-filter'));
      });
    });

    // Phase A.5: cycle pill row (Monthly / Quarterly / … / All). Lives next to
    // the existing filter pills on repricing.tpl + price_decisions.tpl. Filters
    // table rows by their data-cb-cycle="<cycle>" attr (which the templates
    // emit per WHMCS cycle literal).
    $$('[data-cb-filter-cycle]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        var group = btn.parentNode;
        $$('[data-cb-filter-cycle]', group).forEach(function (b) { b.setAttribute('aria-pressed', 'false'); });
        btn.setAttribute('aria-pressed', 'true');
        // Search broader: cycle filter may sit in a separate toolbar above the table.
        var card = btn.closest('.cb-card');
        var table = null;
        if (card) {
          table = card.querySelector('.cb-table');
        }
        if (!table) {
          // Fall back to first table after the toolbar.
          var toolbar = btn.closest('.cb-toolbar');
          var sib = toolbar ? toolbar.nextElementSibling : null;
          while (sib && !table) {
            table = sib.querySelector ? sib.querySelector('.cb-table') : null;
            if (sib.classList && sib.classList.contains('cb-table')) table = sib;
            sib = sib.nextElementSibling;
          }
        }
        if (!table) return;
        applyCycleFilter(table, btn.getAttribute('data-cb-filter-cycle'));
      });
    });
  }

  // ── search ────────────────────────────────────────────────────────────────

  function wireSearch() {
    $$('[data-cb-search]').forEach(function (input) {
      var table = input.closest('.cb-card, section, body').querySelector('.cb-table');
      if (!table) return;
      input.addEventListener('input', debounce(function () {
        var q = input.value.trim().toLowerCase();
        $$('tbody tr', table).forEach(function (tr) {
          if (!q) { tr.hidden = false; return; }
          var hay = (tr.getAttribute('data-cb-profile-name') || '') + ' '
                  + (tr.getAttribute('data-cb-trigger') || '') + ' '
                  + tr.textContent;
          tr.hidden = hay.toLowerCase().indexOf(q) === -1;
        });
      }, 150));
    });
  }

  // ── sort ──────────────────────────────────────────────────────────────────

  function cellValue(tr, idx, key) {
    var tds = tr.children;
    if (!tds || !tds[idx]) return '';
    var tdWithKey = tr.querySelector('td[data-cb-sort-value]');
    if (tdWithKey && tdWithKey.cellIndex === idx) return tdWithKey.getAttribute('data-cb-sort-value');
    return tds[idx].textContent.trim();
  }

  function wireSort() {
    $$('[data-cb-sort]').forEach(function (th) {
      th.addEventListener('click', function () {
        var table = th.closest('table');
        if (!table) return;
        var headers = $$('th', table);
        var idx = headers.indexOf(th);
        var dir = th.getAttribute('data-cb-sort-dir') === 'asc' ? 'desc' : 'asc';
        // reset siblings
        headers.forEach(function (h) {
          h.removeAttribute('data-cb-sort-dir');
          var a = h.querySelector('.arr'); if (a) a.textContent = '';
        });
        th.setAttribute('data-cb-sort-dir', dir);
        var arr = th.querySelector('.arr'); if (arr) arr.textContent = dir === 'asc' ? '▲' : '▼';

        var tbody = table.querySelector('tbody');
        if (!tbody) return;
        var rows = $$('tr', tbody);
        rows.sort(function (a, b) {
          var av = cellValue(a, idx);
          var bv = cellValue(b, idx);
          var an = parseFloat(av), bn = parseFloat(bv);
          var cmp;
          if (!isNaN(an) && !isNaN(bn) && /^-?[\d.,]+$/.test(av.replace(/[€₹$,\s]/g, ''))) {
            cmp = an - bn;
          } else {
            cmp = av.localeCompare(bv);
          }
          return dir === 'asc' ? cmp : -cmp;
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
      });
    });
  }

  // ── bulk select ───────────────────────────────────────────────────────────

  function wireBulk() {
    $$('[data-cb-bulk-all]').forEach(function (master) {
      var table = master.closest('table');
      if (!table) return;
      master.addEventListener('change', function () {
        $$('[data-cb-bulk]', table).forEach(function (cb) {
          if (cb.offsetParent === null) return; // skip hidden rows
          cb.checked = master.checked;
        });
        refreshBulkToolbar(table);
      });
    });
    $$('[data-cb-bulk]').forEach(function (cb) {
      cb.addEventListener('change', function () {
        var table = cb.closest('table');
        if (table) refreshBulkToolbar(table);
      });
    });
  }

  function refreshBulkToolbar(table) {
    // The bulk bar is a sibling card of the table (not inside it), so search
    // the whole document — scoping to the table's card finds nothing.
    var bar = document.querySelector('[data-cb-bulk-toolbar]');
    if (!bar) return;
    var n = $$('[data-cb-bulk]', table).filter(function (cb) { return cb.checked; }).length;
    var countEl = bar.querySelector('[data-cb-bulk-count]');
    if (countEl) countEl.textContent = String(n);
    // Toggle the `hidden` attribute (UA display:none) AND the inline display,
    // mirroring the drawer fix — neither alone wins reliably against the other.
    if (n > 0) {
      bar.removeAttribute('hidden');
      bar.hidden = false;
    } else {
      bar.setAttribute('hidden', '');
      bar.hidden = true;
    }
  }

  // ── modal ─────────────────────────────────────────────────────────────────

  function openModal(id) {
    var m = document.getElementById('cb-modal-' + id);
    if (m) m.classList.add('open');
    return m;
  }
  function closeModal(m) { if (m) m.classList.remove('open'); }
  function closeAllModals() { $$('.cb-modal.open').forEach(closeModal); }

  function wireModals() {
    $$('[data-cb-open-modal]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        var modalId = btn.getAttribute('data-cb-open-modal');
        var m = openModal(modalId);
        // The profile-create modal doubles as the edit modal. If the trigger
        // carries data-cb-profile-edit-id, switch the form into edit mode and
        // prefill from /ajax-profile-edit-form.
        var editId = btn.getAttribute('data-cb-profile-edit-id');
        if (m && modalId === 'profile-create') {
          if (editId) {
            primeProfileEditMode(m, editId);
          } else {
            primeProfileCreateMode(m);
          }
        }
      });
    });
    $$('[data-cb-close-modal]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        var m = btn.closest('.cb-modal');
        closeModal(m);
      });
    });
    $$('.cb-modal').forEach(function (m) {
      m.addEventListener('click', function (e) {
        // Only close when the backdrop itself is clicked.
        if (e.target === m) closeModal(m);
      });
    });
  }

  // ── configurator (per-dimension price calculator inside the profile modal) ─

  // Per-modal state. Key: the modal element. Value: { cfg, plan, period }.
  var cbCfgState = new WeakMap();

  function eur(n) {
    var v = Math.round(Number(n) * 100) / 100;
    var sign = v < 0 ? '-' : '';
    v = Math.abs(v).toFixed(2);
    return sign + '€' + v;
  }
  function r2(n) { return Math.round(Number(n) * 100) / 100; }
  function sgn(n) {
    var v = r2(n);
    if (v === 0) return '€0';
    return (v > 0 ? '+' : '−') + '€' + Math.abs(v).toFixed(2);
  }

  function optText(o) {
    var t = o.label;
    if (o.monthly > 0) t += ' (+€' + Number(o.monthly).toFixed(2) + '/mo)';
    else if (o.monthly < 0) t += ' (−€' + Math.abs(Number(o.monthly)).toFixed(2) + '/mo)';
    else t += ' — included';
    if (o.setup && Number(o.setup) > 0) t += ' +€' + Number(o.setup).toFixed(2) + ' setup';
    return t;
  }

  function getCfgRoot(modal) {
    return modal.querySelector('[data-cb-configurator]');
  }
  function getCfgSummary(modal) {
    return modal.querySelector('[data-cb-summary]');
  }
  function getCfgResetBtn(modal) {
    return modal.querySelector('[data-cb-cfg-reset]');
  }

  function renderConfigurator(modal, cfg) {
    var root = getCfgRoot(modal);
    if (!root) return;
    if (!cfg.controls || cfg.controls.length === 0) {
      root.innerHTML = '<div class="cb-cfg-empty">No configurable dimensions for this plan.</div>';
      return;
    }
    var html = '<div class="cb-cfg-grid">';
    cfg.controls.forEach(function (c, ci) {
      var selectId = 'cb-cfg-sel-' + ci;
      html += '<label for="' + selectId + '">' + escapeHtml(c.label) + '</label>';
      html += '<select id="' + selectId + '" class="cb-field cbcfgsel"'
            + ' data-cb-cfg-control="' + escapeHtml(c.key) + '"'
            + ' data-cb-cfg-control-idx="' + ci + '"'
            + ' data-cb-cfg-default-idx="' + (c.defaultIdx | 0) + '">';
      c.options.forEach(function (o, oi) {
        var sel = (oi === c.defaultIdx) ? ' selected' : '';
        html += '<option value="' + oi + '"'
              + ' data-cb-opt-idx="' + oi + '"'
              + ' data-cb-opt-monthly="' + Number(o.monthly) + '"'
              + ' data-cb-opt-setup="' + Number(o.setup) + '"'
              + ' data-cb-opt-label="' + escapeHtml(o.label) + '"'
              + sel + '>' + escapeHtml(optText(o)) + '</option>';
      });
      html += '</select>';
    });
    html += '</div>';
    root.innerHTML = html;
    var resetBtn = getCfgResetBtn(modal);
    if (resetBtn) resetBtn.hidden = false;
    $$('.cbcfgsel', root).forEach(function (s) {
      s.addEventListener('change', function () { recalcCfg(modal); });
    });
  }

  function recalcCfg(modal) {
    var state = cbCfgState.get(modal);
    if (!state || !state.cfg) return;
    var cfg = state.cfg;
    var periodEl = modal.querySelector('[data-cb-cfg-period]');
    var period = parseInt(periodEl ? periodEl.value : '6', 10) || 6;
    var pk = String(period);
    var anchorM = (cfg.default_monthly_by_period && cfg.default_monthly_by_period[pk] != null)
      ? Number(cfg.default_monthly_by_period[pk]) : 0;
    var anchorS = (cfg.default_setup_by_period && cfg.default_setup_by_period[pk] != null)
      ? Number(cfg.default_setup_by_period[pk]) : 0;
    var mD = 0, sD = 0;
    var changes = [], selected = [];
    var serialised = {};

    cfg.controls.forEach(function (c, ci) {
      var selectEl = modal.querySelector('select[data-cb-cfg-control-idx="' + ci + '"]');
      if (!selectEl) return;
      var selI = parseInt(selectEl.value, 10) || 0;
      var sel = c.options[selI] || c.options[0];
      var def = c.options[c.defaultIdx] || c.options[0];
      mD += Number(sel.monthly) - Number(def.monthly);
      sD += Number(sel.setup) - Number(def.setup);
      selected.push(c.label + '=' + sel.label);
      serialised[c.key] = {
        label: sel.label,
        monthly: Number(sel.monthly),
        setup: Number(sel.setup),
        is_default: selI === c.defaultIdx,
      };
      if (selI !== c.defaultIdx) {
        changes.push({
          label: c.label,
          from: def.label,
          to: sel.label,
          dm: r2(Number(sel.monthly) - Number(def.monthly)),
          ds: r2(Number(sel.setup) - Number(def.setup)),
        });
      }
    });

    var cfgM = r2(anchorM + mD);
    var cfgS = r2(anchorS + sD);
    var tot = r2(cfgM * period + cfgS);
    var summary = getCfgSummary(modal);
    if (summary) {
      summary.hidden = false;
      var h = '<div class="ln muted"><span>Default (' + period + ' mo)</span><span>' + eur(anchorM) + '/mo</span></div>';
      for (var i = 0; i < changes.length; i++) {
        var ch = changes[i];
        var up = ch.dm > 0;
        h += '<div class="ln chg' + (up ? ' up' : '') + '">'
           + '<span>' + escapeHtml(ch.label) + ': ' + escapeHtml(ch.to) + '</span>'
           + '<span class="v">' + sgn(ch.dm) + '/mo'
           + (ch.ds ? ' · ' + sgn(ch.ds) + ' setup' : '')
           + '</span></div>';
      }
      h += '<div class="ln tot"><span>Configured monthly</span><span class="v">' + eur(cfgM) + '/mo</span></div>';
      h += '<div class="ln"><span>Setup (one-time)</span><span>' + (cfgS > 0 ? eur(cfgS) : '—') + '</span></div>';
      h += '<div class="ln"><span>Billed total (' + period + ' mo)</span><span>' + eur(tot) + '</span></div>';
      h += '<div class="sel"><b>Selected:</b> ' + escapeHtml(selected.join(' · ')) + '</div>';
      summary.innerHTML = h;
    }

    var jsonEl = modal.querySelector('[data-cb-options-json]');
    if (jsonEl) jsonEl.value = JSON.stringify(serialised);
    // OS + Region are derived server-side from the options JSON above
    // (single source of truth) — no hidden region/os inputs to mirror.
  }

  function loadConfiguratorForModal(modal, opts) {
    opts = opts || {};
    var planEl = modal.querySelector('[data-cb-cfg-plan]');
    var plan = planEl ? planEl.value : '';
    if (!plan) {
      var root = getCfgRoot(modal);
      if (root) root.innerHTML = '<div class="cb-cfg-empty">Pick a plan to load configuration options…</div>';
      var summary = getCfgSummary(modal);
      if (summary) summary.hidden = true;
      var resetBtn = getCfgResetBtn(modal);
      if (resetBtn) resetBtn.hidden = true;
      return Promise.resolve(null);
    }
    var root2 = getCfgRoot(modal);
    if (root2) root2.innerHTML = '<div class="cb-cfg-empty">Loading configurator…</div>';
    return ajax('GET', 'ajax-configurator', { plan_slug: plan }).then(function (j) {
      if (j.error) {
        if (root2) root2.innerHTML = '<div class="cb-cfg-empty">Configurator failed: ' + escapeHtml(j.error) + '</div>';
        var summary2 = getCfgSummary(modal);
        if (summary2) summary2.hidden = true;
        cbCfgState.delete(modal);
        return null;
      }
      cbCfgState.set(modal, { cfg: j, plan: plan });
      renderConfigurator(modal, j);
      // Apply prefill selections (edit mode) before the first recalc so the
      // summary reflects the saved profile, not the defaults.
      if (opts.prefill && typeof opts.prefill === 'object') {
        applyConfiguratorPrefill(modal, opts.prefill);
      }
      recalcCfg(modal);
      updatePublishCyclesPreview(modal);
      return j;
    });
  }

  function applyConfiguratorPrefill(modal, prefill) {
    var state = cbCfgState.get(modal);
    if (!state || !state.cfg) return;
    state.cfg.controls.forEach(function (c, ci) {
      var sel = prefill[c.key];
      if (!sel || !sel.label) return;
      var selectEl = modal.querySelector('select[data-cb-cfg-control-idx="' + ci + '"]');
      if (!selectEl) return;
      for (var i = 0; i < c.options.length; i++) {
        if (c.options[i].label === sel.label) {
          selectEl.value = String(i);
          break;
        }
      }
    });
  }

  // ── Publish cycles (profile = source) ───────────────────────────────────
  // OR the checked cycle bits into the hidden published_cycles_mask. Empty
  // selection falls back to 63 (all) so a profile never sources nothing.
  function syncPublishedMask(modal) {
    var hidden = modal.querySelector('[data-cb-published-mask]');
    if (!hidden) return;
    var mask = 0;
    modal.querySelectorAll('[data-cb-publish-cycle]').forEach(function (cb) {
      if (cb.checked) mask |= parseInt(cb.getAttribute('data-cb-bit'), 10) || 0;
    });
    hidden.value = String(mask === 0 ? 63 : mask);
  }

  // Source (cost) price per cycle, EUR/mo, from the configurator's periods map.
  // Fallback rule (mirrors SyncEngine): source(M) = effective_monthly of the
  // longest available period whose months ≤ M (24/36 → 12-mo; missing 3 → 1).
  function updatePublishCyclesPreview(modal) {
    var state = cbCfgState.get(modal);
    var periods = (state && state.cfg && state.cfg.periods) ? state.cfg.periods : null;
    var scraped = {};
    if (periods) {
      Object.keys(periods).forEach(function (k) {
        var m = parseInt(k, 10);
        var row = periods[k] || {};
        if (m > 0 && row.effective_monthly != null) scraped[m] = parseFloat(row.effective_monthly);
      });
    }
    var avail = Object.keys(scraped).map(function (k) { return parseInt(k, 10); }).sort(function (a, b) { return a - b; });

    modal.querySelectorAll('[data-cb-cycle-source]').forEach(function (span) {
      var target = parseInt(span.getAttribute('data-cb-cycle-source'), 10);
      if (!avail.length) { span.textContent = '—'; return; }
      var best = null;
      avail.forEach(function (m) { if (m <= target && (best === null || m > best)) best = m; });
      if (best === null) best = avail[0];
      span.textContent = '€' + scraped[best].toFixed(2) + '/mo';
    });
  }

  // Reshape the form to the selected mode. Fixed = pre-packaged SKU: the admin
  // locks every option and NO configurable options are exposed to customers, so
  // the expose toggle is hidden. Configurable = customer picks exposed options.
  function applyModeUi(modal) {
    var modeSel = modal.querySelector('[data-cb-profile-mode]');
    var mode = modeSel ? modeSel.value : 'fixed_admin_profile';
    var isFixed = (mode !== 'customer_configurable_product');
    var exposeField = modal.querySelector('[data-cb-expose-field]');
    if (exposeField) exposeField.hidden = isFixed;
    var hint = modal.querySelector('[data-cb-mode-hint]');
    if (hint) {
      hint.textContent = isFixed
        ? 'Pre-packaged plan: pick a value for every option below — that locked set becomes the SKU. Customers cannot change it.'
        : 'Configurable product: customers choose the options you expose. Curate exposure via the Exposure editor after saving.';
    }
  }

  function primeProfileCreateMode(modal) {
    var titleEl = modal.querySelector('[data-cb-modal-title]');
    var actionEl = modal.querySelector('[data-cb-form-action]');
    var idEl = modal.querySelector('[data-cb-form-id]');
    var submitEl = modal.querySelector('[data-cb-submit-label]');
    var nameEl = modal.querySelector('input[name="name"]');
    var tagsEl = modal.querySelector('input[name="tags"]');
    var stratEl = modal.querySelector('select[name="sync_strategy"]');
    var modeEl = modal.querySelector('[data-cb-profile-mode]');
    var exposeEl = modal.querySelector('[data-cb-expose-config]');
    if (titleEl) titleEl.textContent = 'Create profile';
    if (submitEl) submitEl.textContent = 'Create profile';
    if (actionEl) actionEl.value = 'profile-create';
    if (idEl) { idEl.disabled = true; idEl.value = ''; }
    if (nameEl) nameEl.value = '';
    if (tagsEl) tagsEl.value = '';
    if (stratEl) stratEl.value = 'notify';
    if (modeEl) modeEl.value = 'fixed_admin_profile';
    if (exposeEl) exposeEl.checked = true;
    // Default: source all six cycles (the mapping narrows to customer-facing).
    modal.querySelectorAll('[data-cb-publish-cycle]').forEach(function (cb) { cb.checked = true; });
    syncPublishedMask(modal);
    applyModeUi(modal);
    var planEl = modal.querySelector('[data-cb-cfg-plan]');
    if (planEl) planEl.value = '';
    loadConfiguratorForModal(modal);
  }

  function primeProfileEditMode(modal, profileId) {
    var titleEl = modal.querySelector('[data-cb-modal-title]');
    var actionEl = modal.querySelector('[data-cb-form-action]');
    var idEl = modal.querySelector('[data-cb-form-id]');
    var submitEl = modal.querySelector('[data-cb-submit-label]');
    if (titleEl) titleEl.textContent = 'Edit profile #' + profileId;
    if (submitEl) submitEl.textContent = 'Save profile';
    if (actionEl) actionEl.value = 'profile-save';
    if (idEl) { idEl.disabled = false; idEl.value = String(profileId); }

    ajax('GET', 'ajax-profile-edit-form', { id: profileId }).then(function (j) {
      if (j.error) { cbToast('Edit load failed: ' + j.error, 'bad'); return; }
      var p = j.profile || {};
      var sel = j.selections || {};
      var nameEl = modal.querySelector('input[name="name"]');
      var planEl = modal.querySelector('[data-cb-cfg-plan]');
      var periodEl = modal.querySelector('[data-cb-cfg-period]');
      var tagsEl = modal.querySelector('input[name="tags"]');
      var stratEl = modal.querySelector('select[name="sync_strategy"]');
      var modeEl = modal.querySelector('[data-cb-profile-mode]');
      var exposeEl = modal.querySelector('[data-cb-expose-config]');
      if (nameEl) nameEl.value = p.name || '';
      if (planEl) planEl.value = p.plan_slug || '';
      if (periodEl) periodEl.value = String(p.period_months || 6);
      if (tagsEl) tagsEl.value = p.tags || '';
      if (stratEl && p.sync_strategy) stratEl.value = p.sync_strategy;
      if (modeEl && p.profile_mode) modeEl.value = p.profile_mode;
      // null/undefined (pre-v7 rows) → default exposed; explicit "0" → unchecked.
      if (exposeEl) exposeEl.checked = String(p.expose_configurable_options) !== '0';
      // Publish cycles: restore from saved mask (pre-v8 rows → all six).
      var savedMask = (p.published_cycles_mask == null || p.published_cycles_mask === '')
        ? 63 : (parseInt(p.published_cycles_mask, 10) || 63);
      modal.querySelectorAll('[data-cb-publish-cycle]').forEach(function (cb) {
        var bit = parseInt(cb.getAttribute('data-cb-bit'), 10) || 0;
        cb.checked = (savedMask & bit) !== 0;
      });
      syncPublishedMask(modal);
      applyModeUi(modal);
      loadConfiguratorForModal(modal, { prefill: sel });
    });
  }

  function wireConfiguratorForms() {
    $$('[data-cb-configurator-form]').forEach(function (form) {
      var modal = form.closest('.cb-modal');
      if (!modal) return;
      var planEl = modal.querySelector('[data-cb-cfg-plan]');
      var periodEl = modal.querySelector('[data-cb-cfg-period]');
      var resetBtn = modal.querySelector('[data-cb-cfg-reset]');

      var debouncedLoad = debounce(function () { loadConfiguratorForModal(modal); }, 200);
      if (planEl) planEl.addEventListener('change', debouncedLoad);
      if (periodEl) periodEl.addEventListener('change', function () {
        // Period change doesn't require a re-fetch — anchors are in the cfg
        // we already loaded. Just recompute.
        if (cbCfgState.get(modal)) recalcCfg(modal);
        else debouncedLoad();
      });

      // Publish-cycle checkboxes maintain the hidden published_cycles_mask.
      modal.querySelectorAll('[data-cb-publish-cycle]').forEach(function (cb) {
        cb.addEventListener('change', function () { syncPublishedMask(modal); });
      });
      // Mode toggle reshapes the form (fixed = pre-packaged, no exposure).
      var modeSel = modal.querySelector('[data-cb-profile-mode]');
      if (modeSel) modeSel.addEventListener('change', function () { applyModeUi(modal); });
      if (resetBtn) {
        resetBtn.addEventListener('click', function (e) {
          e.preventDefault();
          var state = cbCfgState.get(modal);
          if (!state || !state.cfg) return;
          state.cfg.controls.forEach(function (c, ci) {
            var s = modal.querySelector('select[data-cb-cfg-control-idx="' + ci + '"]');
            if (s) s.value = String(c.defaultIdx);
          });
          recalcCfg(modal);
        });
      }
      // On submit, force a final recalc so the JSON payload reflects the
      // current selections even if a change event was missed.
      form.addEventListener('submit', function () {
        if (cbCfgState.get(modal)) recalcCfg(modal);
      });
    });
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  // ── live quote preview ────────────────────────────────────────────────────

  function wireQuotePreview() {
    var planEl = $('[data-cb-quote-plan]');
    var periodEl = $('[data-cb-quote-period]');
    var target = $('[data-cb-preview-price]');
    if (!planEl || !periodEl || !target) return;

    var run = debounce(function () {
      var plan = planEl.value;
      var period = parseInt(periodEl.value, 10) || 1;
      if (!plan) { target.textContent = ''; return; }
      target.textContent = 'Calculating…';
      ajax('POST', 'ajax-quote', { plan_slug: plan, period_months: period }).then(function (j) {
        if (j.error) { target.innerHTML = '<span class="cb-pill bad">' + escapeHtml(j.error) + '</span>'; return; }
        var eur = j.base_monthly_eur != null ? j.base_monthly_eur : (j.configured_monthly_eur || '?');
        var final = j.final_monthly != null ? j.final_monthly : '?';
        var cur = j.currency_iso || '';
        var gst = j.gst_pct != null ? j.gst_pct : '';
        var fxm = j.fx_markup_pct != null ? j.fx_markup_pct : '';
        target.innerHTML = 'Preview: <span class="mono">€' + escapeHtml(String(eur)) + '/mo</span> '
          + '→ <span class="price">' + escapeHtml(String(final)) + ' ' + escapeHtml(cur) + '/mo</span> '
          + '<span class="muted">(incl. ' + escapeHtml(String(gst)) + '% GST + ' + escapeHtml(String(fxm)) + '% FX markup)</span>';
      }).catch(function () {
        target.innerHTML = '<span class="cb-pill bad">Preview failed</span>';
      });
    }, 250);

    planEl.addEventListener('change', run);
    planEl.addEventListener('input', run);
    periodEl.addEventListener('change', run);
    periodEl.addEventListener('input', run);
    // Initial render if both pre-populated
    if (planEl.value) run();
  }

  // ── FX preview ────────────────────────────────────────────────────────────

  function fetchFxOnce() {
    var target = $('[data-cb-fx-preview]');
    if (!target) return;
    ajax('GET', 'ajax-fx', null).then(function (j) {
      if (j.error) { target.innerHTML = '<span class="cb-pill bad">FX error: ' + escapeHtml(j.error) + '</span>'; return; }
      var rate = j.rate != null ? j.rate : (j.mid != null ? j.mid : '?');
      var src = j.source || j.provider || 'mid-market';
      var age = j.age_minutes != null ? (' · ' + j.age_minutes + ' min old') : '';
      target.innerHTML = '<span class="cb-pill good">1 EUR = <span class="mono">' + escapeHtml(String(rate)) + '</span> INR</span> '
        + '<span class="muted">' + escapeHtml(src) + escapeHtml(age) + '</span>';
    }).catch(function () {
      target.innerHTML = '<span class="cb-pill bad">FX unreachable</span>';
    });
  }

  function wireFxPreview() {
    if (!$('[data-cb-fx-preview]')) return;
    fetchFxOnce();
    setInterval(fetchFxOnce, 60 * 1000);
  }

  // ── test API connection ───────────────────────────────────────────────────

  function wireTestApi() {
    $$('[data-cb-action="test-api-connection"]').forEach(function (btn) {
      var result = btn.parentNode ? btn.parentNode.querySelector('[data-cb-result]') : null;
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        btn.disabled = true;
        var origLabel = btn.textContent;
        btn.textContent = 'Testing…';
        if (result) {
          result.className = 'cb-inline-result cb-tone-muted';
          result.textContent = 'Checking provider catalog API…';
        }
        ajax('POST', 'ajax-meta-probe', {}).then(function (j) {
          btn.disabled = false;
          btn.textContent = origLabel;
          if (j.ok) {
            if (result) {
              result.className = 'cb-inline-result cb-tone-good';
              result.textContent = 'API reachable · scraper ' + (j.scraper_version || '?') + ' · ' + (j.snapshot_at || '');
            }
          } else {
            if (result) {
              result.className = 'cb-inline-result cb-tone-bad';
              result.textContent = 'API error: ' + (j.error || 'unknown');
            }
          }
        }).catch(function (err) {
          btn.disabled = false;
          btn.textContent = origLabel;
          if (result) {
            result.className = 'cb-inline-result cb-tone-bad';
            result.textContent = 'API unreachable: ' + String(err);
          }
        });
      });
    });
  }

  // ── sparklines ────────────────────────────────────────────────────────────

  function renderSparkline(svg, values, opts) {
    opts = opts || {};
    if (!values || values.length === 0) {
      svg.innerHTML = '<text x="2" y="14" font-size="9" fill="currentColor" opacity=".4">no data</text>';
      return;
    }
    var w = svg.clientWidth || 80, h = svg.clientHeight || 22;
    var pad = 2;
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    var range = Math.max(0.0001, max - min);
    var stepX = values.length > 1 ? (w - pad * 2) / (values.length - 1) : 0;
    var pts = values.map(function (v, i) {
      var x = pad + i * stepX;
      var y = h - pad - ((v - min) / range) * (h - pad * 2);
      return x.toFixed(1) + ',' + y.toFixed(1);
    }).join(' ');
    var html = '';
    if (opts.large) {
      // gridlines
      for (var i = 1; i <= 3; i++) {
        var gy = (h / 4) * i;
        html += '<line x1="0" x2="' + w + '" y1="' + gy + '" y2="' + gy + '" stroke="currentColor" stroke-opacity=".07"/>';
      }
      html += '<text x="2" y="10" font-size="9" fill="currentColor" opacity=".55">' + max.toFixed(2) + '</text>';
      html += '<text x="2" y="' + (h - 2) + '" font-size="9" fill="currentColor" opacity=".55">' + min.toFixed(2) + '</text>';
    }
    html += '<polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="' + pts + '"/>';
    svg.setAttribute('viewBox', '0 0 ' + w + ' ' + h);
    svg.innerHTML = html;
  }

  function wireSparklines() {
    var nodes = $$('[data-cb-sparkline], [data-cb-sparkline-large]');
    // Group by profile id, fetch once per id.
    var byId = {};
    nodes.forEach(function (n) {
      var id = n.getAttribute('data-cb-profile-id');
      if (!id) return;
      (byId[id] = byId[id] || []).push(n);
    });
    Object.keys(byId).forEach(function (id) {
      ajax('GET', 'ajax-profile-versions', { id: id }).then(function (j) {
        if (j.error || !Array.isArray(j.versions)) return;
        // API returns newest first per spec; reverse to chronological.
        var values = j.versions.slice().reverse().map(function (v) {
          return parseFloat(v.final_monthly);
        }).filter(function (n) { return !isNaN(n); });
        byId[id].forEach(function (svg) {
          var large = svg.hasAttribute('data-cb-sparkline-large');
          renderSparkline(svg, values, { large: large });
        });
      }).catch(function () { /* silent */ });
    });
  }

  // ── mapping form (Phase A.5 cycle table) ──────────────────────────────────
  //
  // On product or currency change the form fetches /ajax-product-cycles and
  // re-hydrates the 6-row cycle table: catalog-sync checkbox is disabled when
  // the cycle status is 'disabled' (and respect_disabled_cycles is on) or
  // 'absent'; renewal-reprice checkbox is always enabled. Each checkbox toggle
  // recomputes the hidden bitmask inputs; markup overrides are JSON-encoded
  // into a hidden field. Form submit fires a final recompute so missed change
  // events don't poison the payload.
  function recomputeMask(scope, attr) {
    var m = 0;
    $$('[' + attr + ']:checked', scope).forEach(function (cb) {
      var bit = parseInt(cb.getAttribute(attr), 10);
      if (!isNaN(bit) && bit >= 0) m |= (1 << bit);
    });
    return m;
  }

  function refreshMappingMasks(scope) {
    var catalog = recomputeMask(scope, 'data-cb-cycle-bit-catalog');
    var renewal = recomputeMask(scope, 'data-cb-cycle-bit-renewal');
    var catalogInput = scope.querySelector('[data-cb-catalog-mask]');
    var renewalInput = scope.querySelector('[data-cb-renewal-mask]');
    if (catalogInput) catalogInput.value = String(catalog);
    if (renewalInput) renewalInput.value = String(renewal);
    refreshMappingPreview(scope, catalog, renewal);
  }

  function refreshMarkupOverridesJson(scope) {
    var input = scope.querySelector('[data-cb-markup-overrides-json]');
    if (!input) return;
    var out = {};
    $$('[data-cb-markup-strategy]', scope).forEach(function (sel) {
      var cycle = sel.getAttribute('data-cb-markup-strategy');
      var strategy = sel.value;
      if (!strategy || strategy === 'inherit') return;
      var valEl = scope.querySelector('[data-cb-markup-value="' + cssAttrEscape(cycle) + '"]');
      var val = valEl ? parseFloat(valEl.value) : NaN;
      if (isNaN(val)) return;
      out[cycle] = { strategy: strategy, value: val };
    });
    input.value = JSON.stringify(out);
  }

  function cssAttrEscape(s) {
    // CSS.escape isn't on PHP-served IE/old WHMCS browsers; build a tiny
    // escape for the chars we actually use ("Semi-Annually" is the worst
    // case — the hyphen is fine).
    return String(s).replace(/["\\]/g, '\\$&');
  }

  function refreshMappingPreview(scope, catalog, renewal) {
    var body = scope.querySelector('[data-cb-mapping-preview-body]');
    if (!body) return;
    if (catalog === 0 && renewal === 0) {
      body.innerHTML = '<span class="muted cb-text-sm">No cycles selected yet — pick at least one catalog or renewal cycle to save.</span>';
      return;
    }
    var html = '<div class="cb-pill-row">';
    var cycles = ['Monthly', 'Quarterly', 'Semi-Annually', 'Annually', 'Biennially', 'Triennially'];
    for (var i = 0; i < cycles.length; i++) {
      var bit = 1 << i;
      var inC = (catalog & bit) !== 0;
      var inR = (renewal & bit) !== 0;
      if (!inC && !inR) continue;
      var tone = (inC && inR) ? 'good' : (inC ? 'warn' : 'grey');
      var suffix = (inC && inR) ? '' : (inC ? ' · catalog' : ' · renewal');
      html += '<span class="cb-pill ' + tone + '">' + escapeHtml(cycles[i].toLowerCase() + suffix) + '</span>';
    }
    html += '</div>';
    body.innerHTML = html;
  }

  // Source (cost) EUR/mo for a target cycle: nearest available period ≤ target
  // months (24/36 → 12-mo, missing 3 → 1-mo). Mirrors the engine fallback.
  function sourceRateForMonths(sourceEur, target) {
    var avail = Object.keys(sourceEur || {}).map(function (k) { return parseInt(k, 10); })
      .filter(function (m) { return m > 0; }).sort(function (a, b) { return a - b; });
    if (!avail.length) return null;
    var best = null;
    avail.forEach(function (m) { if (m <= target && (best === null || m > best)) best = m; });
    if (best === null) best = avail[0];
    return parseFloat(sourceEur[best]);
  }

  function renderCycleTableRows(scope, cycles, respectDisabled, sourceEur) {
    cycles.forEach(function (c) {
      var sel = '[data-cb-cycle-row="' + cssAttrEscape(c.cycle) + '"]';
      var tr = scope.querySelector(sel);
      if (!tr) return;
      tr.setAttribute('data-cb-cycle-status', c.status);

      var srcCell = tr.querySelector('[data-cb-cycle-source]');
      if (srcCell) {
        var months = parseInt(tr.getAttribute('data-cb-cycle-months'), 10) || 0;
        var rate = sourceRateForMonths(sourceEur || {}, months);
        srcCell.innerHTML = (rate === null)
          ? '<span class="muted cb-text-xs">—</span>'
          : '<span class="mono" title="source cost / mo × ' + months + ' mo">€' + rate.toFixed(2) + '/mo · €' + (rate * months).toFixed(2) + '</span>';
      }

      var priceCell = tr.querySelector('[data-cb-cycle-current-price]');
      if (priceCell) {
        var html = '';
        if (c.current_price === null || c.current_price === undefined) {
          html = '<span class="muted cb-text-xs">absent</span>';
        } else if (c.status === 'disabled') {
          html = '<span class="cb-pill grey" title="WHMCS sentinel: -1">disabled</span>';
        } else if (c.status === 'free') {
          html = '<span class="cb-pill warn">free (0.00)</span>';
        } else {
          html = '<span class="price">' + escapeHtml(Number(c.current_price).toFixed(2)) + '</span>';
        }
        priceCell.innerHTML = html;
      }

      var catalogCb = tr.querySelector('[data-cb-cycle-bit-catalog]');
      if (catalogCb) {
        var canCatalog = !!c.can_catalog_sync;
        if (c.status === 'disabled' && !respectDisabled) canCatalog = true;
        if (c.status === 'absent') canCatalog = false;
        catalogCb.disabled = !canCatalog;
        if (!canCatalog) {
          catalogCb.checked = false;
          catalogCb.title = 'Catalog sync blocked: status = ' + c.status;
        } else {
          catalogCb.title = '';
        }
      }
      var renewalCb = tr.querySelector('[data-cb-cycle-bit-renewal]');
      if (renewalCb) {
        renewalCb.disabled = !c.can_renewal_sync;
      }
    });
  }

  function loadMappingCycles(scope) {
    var productEl = scope.querySelector('[data-cb-product-id]');
    var currencyEl = scope.querySelector('[data-cb-currency-id]');
    var loading = scope.querySelector('[data-cb-cycles-loading]');
    var productId = productEl ? parseInt(productEl.value, 10) : 0;
    if (!productId) {
      if (loading) {
        loading.textContent = 'Pick a product to load catalog prices…';
        loading.hidden = false;
        loading.classList.remove('cb-tone-bad');
      }
      return;
    }
    var profileEl = scope.querySelector('[data-cb-mapping-profile]');
    var params = { product_id: productId };
    if (currencyEl && currencyEl.value) params.currency_id = parseInt(currencyEl.value, 10);
    if (profileEl && profileEl.value) params.profile_id = parseInt(profileEl.value, 10);
    if (loading) {
      loading.textContent = 'Loading catalog prices…';
      loading.hidden = false;
      loading.classList.remove('cb-tone-bad');
    }
    var respectDisabledEl = scope.querySelector('[data-cb-respect-disabled]');
    var respectDisabled = respectDisabledEl ? respectDisabledEl.checked : true;
    ajax('GET', 'ajax-product-cycles', params).then(function (j) {
      if (j.error) {
        if (loading) {
          loading.textContent = 'Could not load: ' + j.error;
          loading.classList.add('cb-tone-bad');
        }
        return;
      }
      if (loading) {
        loading.hidden = true;
      }
      renderCycleTableRows(scope, j.cycles || [], respectDisabled, j.source_eur || {});
      refreshMappingMasks(scope);
    }).catch(function (err) {
      if (loading) {
        loading.textContent = 'Network error: ' + String(err);
        loading.classList.add('cb-tone-bad');
      }
    });
  }

  function wireMappingForm() {
    $$('[data-cb-mapping-form-scope]').forEach(function (scope) {
      var productEl = scope.querySelector('[data-cb-product-id]');
      var currencyEl = scope.querySelector('[data-cb-currency-id]');
      var respectEl = scope.querySelector('[data-cb-respect-disabled]');
      var mapProfileEl = scope.querySelector('[data-cb-mapping-profile]');
      var form = scope.querySelector('form[data-cb-form="mapping-save"]');

      var debouncedLoad = debounce(function () { loadMappingCycles(scope); }, 200);
      if (productEl)  productEl.addEventListener('change', debouncedLoad);
      if (currencyEl) currencyEl.addEventListener('change', debouncedLoad);
      if (respectEl)  respectEl.addEventListener('change', debouncedLoad);
      // Profile drives the Source column (its per-cycle cost vector).
      if (mapProfileEl) mapProfileEl.addEventListener('change', debouncedLoad);

      // Mask + JSON recompute as the admin toggles per-cycle checkboxes.
      $$('[data-cb-cycle-bit-catalog], [data-cb-cycle-bit-renewal]', scope).forEach(function (cb) {
        cb.addEventListener('change', function () { refreshMappingMasks(scope); });
      });

      // "+ Override" reveals the per-cycle markup editor.
      $$('[data-cb-markup-toggle]', scope).forEach(function (btn) {
        btn.addEventListener('click', function (e) {
          e.preventDefault();
          var cyc = btn.getAttribute('data-cb-markup-toggle');
          var ed = scope.querySelector('[data-cb-markup-editor="' + cssAttrEscape(cyc) + '"]');
          if (!ed) return;
          var open = !ed.hidden;
          ed.hidden = open;
          btn.textContent = open ? '+ Override' : '− Hide';
        });
      });
      $$('[data-cb-markup-strategy], [data-cb-markup-value]', scope).forEach(function (el) {
        el.addEventListener('change', function () { refreshMarkupOverridesJson(scope); });
        el.addEventListener('input',  function () { refreshMarkupOverridesJson(scope); });
      });

      // Edit button on the right-hand "Existing mappings" table — copies
      // profile/product into the left form + triggers a cycles reload. Mask
      // checkboxes are repopulated from the row's stored masks.
      $$('[data-cb-edit-mapping-id]').forEach(function (btn) {
        btn.addEventListener('click', function (e) {
          e.preventDefault();
          var profileId = btn.getAttribute('data-cb-edit-profile-id');
          var productId = btn.getAttribute('data-cb-edit-product-id');
          var profileEl = scope.querySelector('[data-cb-mapping-profile]');
          var pEl = scope.querySelector('[data-cb-product-id]');
          if (profileEl && profileId) profileEl.value = String(profileId);
          if (pEl && productId)        pEl.value      = String(productId);

          var row = btn.closest('[data-cb-mapping-row]');
          var catalogMask = row ? parseInt(row.getAttribute('data-cb-mapping-catalog-mask') || '0', 10) : 0;
          var renewalMask = row ? parseInt(row.getAttribute('data-cb-mapping-renewal-mask') || '0', 10) : 0;
          $$('[data-cb-cycle-bit-catalog]', scope).forEach(function (cb) {
            var bit = parseInt(cb.getAttribute('data-cb-cycle-bit-catalog'), 10);
            cb.checked = !cb.disabled && ((catalogMask & (1 << bit)) !== 0);
          });
          $$('[data-cb-cycle-bit-renewal]', scope).forEach(function (cb) {
            var bit = parseInt(cb.getAttribute('data-cb-cycle-bit-renewal'), 10);
            cb.checked = !cb.disabled && ((renewalMask & (1 << bit)) !== 0);
          });
          // Load the latest catalog prices so we can show the actual current_price
          // alongside the loaded masks.
          loadMappingCycles(scope);
          refreshMappingMasks(scope);
        });
      });

      // Belt + braces: final recompute on submit so any missed change events
      // don't poison the payload.
      if (form) {
        form.addEventListener('submit', function () {
          refreshMappingMasks(scope);
          refreshMarkupOverridesJson(scope);
        });
      }
    });
  }

  // ── keyboard shortcuts ────────────────────────────────────────────────────

  function wireShortcuts() {
    document.addEventListener('keydown', function (e) {
      // Esc always closes overlays.
      if (e.key === 'Escape') { closeAllModals(); return; }
      // Skip if user is typing.
      var tag = (e.target && e.target.tagName || '').toUpperCase();
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || e.target.isContentEditable) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      if (e.key === '/') {
        var s = $('[data-cb-search]');
        if (s) { e.preventDefault(); s.focus(); s.select && s.select(); }
      } else if (e.key === 'n') {
        var n = $('[data-cb-open-modal="profile-create"]');
        if (n) { e.preventDefault(); n.click(); }
      } else if (e.key === 'r') {
        var r = $('[data-cb-action="test-api-connection"]');
        if (r) { e.preventDefault(); r.click(); }
      }
    });
  }

  // ── init ──────────────────────────────────────────────────────────────────

  function init() {
    wireConfirmations();
    wireFilterPills();
    wireSearch();
    wireSort();
    wireBulk();
    wireModals();
    wireQuotePreview();
    wireConfiguratorForms();
    wireFxPreview();
    wireTestApi();
    wireSparklines();
    wireMappingForm();
    wireShortcuts();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
