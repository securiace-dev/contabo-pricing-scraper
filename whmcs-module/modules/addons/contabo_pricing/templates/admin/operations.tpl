<?php require __DIR__ . '/_layout_open.tpl'; ?>
<?php
$cb_counts = is_array($counts ?? null) ? $counts : [];
$cb_operations = is_array($operations ?? null) ? $operations : [];
$cb_accounts = is_array($provider_accounts ?? null) ? $provider_accounts : [];
$cb_capabilities = is_array($capabilities ?? null) ? $capabilities : [];
$cb_findings = is_array($reconciliation ?? null) ? $reconciliation : [];
$cb_adoption = is_array($adoption ?? null) ? $adoption : [];
$cb_commands = is_array($commands ?? null) ? $commands : [];
$cb_billing_sagas = is_array($billing_sagas ?? null) ? $billing_sagas : [];
$cb_communications = is_array($communications ?? null) ? $communications : [];
$cb_capability_switches = is_array($capability_write_settings ?? null)
    ? $capability_write_settings
    : [];
$cb_state_tone = static function (string $state): string {
    if (in_array($state, ['succeeded', 'completed', 'verified', 'supported'], true)) return 'good';
    if (in_array($state, ['failed_terminal', 'rejected', 'conflict', 'missing_upstream'], true)) return 'bad';
    if (in_array($state, ['unknown_outcome', 'manual_review', 'ambiguous', 'failed_retryable'], true)) return 'warn';
    return 'grey';
};
?>

<header class="cb-page-header">
  <div>
    <h2 class="display">VPS operations</h2>
    <p class="cb-card-sub">
      Durable operation status, provider capability certification, reconciliation,
      adoption evidence and append-only recovery commands.
    </p>
  </div>
  <span class="cb-pill <?= !empty($global_writes_enabled) ? 'warn' : 'good' ?>">
    Provider writes <?= !empty($global_writes_enabled) ? 'enabled' : 'stopped' ?>
  </span>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash" role="status"><?= $esc($flash) ?></div>
<?php endif; ?>

<div class="cb-strip" aria-label="Operation status summary">
  <div class="cb-stat">
    <div class="lbl">In progress</div>
    <div class="v"><?= (int) (($cb_counts['accepted'] ?? 0) + ($cb_counts['claimed'] ?? 0) + ($cb_counts['submitted'] ?? 0) + ($cb_counts['provider_pending'] ?? 0)) ?></div>
    <div class="sub">Accepted through provider pending</div>
  </div>
  <div class="cb-stat warn">
    <div class="lbl">Unknown / review</div>
    <div class="v"><?= (int) (($cb_counts['unknown_outcome'] ?? 0) + ($cb_counts['manual_review'] ?? 0)) ?></div>
    <div class="sub">Requires reconciliation or operator decision</div>
  </div>
  <div class="cb-stat bad">
    <div class="lbl">Failed</div>
    <div class="v"><?= (int) (($cb_counts['failed_retryable'] ?? 0) + ($cb_counts['failed_terminal'] ?? 0)) ?></div>
    <div class="sub">Retryable and terminal failures</div>
  </div>
  <div class="cb-stat">
    <div class="lbl">Open findings</div>
    <div class="v"><?= (int) ($cb_counts['open_findings'] ?? 0) ?></div>
    <div class="sub">Drift, missing or orphan-risk findings</div>
  </div>
</div>

<div class="cb-workbench-grid">
  <section class="cb-card">
    <h3 class="cb-card-title">Provider write controls</h3>
    <p class="cb-card-sub">
      Changes are queued for provisioning-cron validation. Disabling is immediate
      on the next cron claim; enabling requires typed confirmation.
    </p>
    <form method="post" action="<?= $esc($module_link) ?>" class="cb-stack">
      <input type="hidden" name="action" value="provider-write-control">
      <input type="hidden" name="scope" value="global">
      <?= generate_token() ?>
      <label class="cb-check">
        <input type="checkbox" name="enabled" value="1"<?= !empty($global_writes_enabled) ? ' checked' : '' ?>>
        <span>Enable all certified provider mutations</span>
      </label>
      <div class="cb-field">
        <label for="cb-global-write-confirmation">To enable, type ENABLE PROVIDER WRITES</label>
        <input type="text" id="cb-global-write-confirmation" name="confirmation"
               autocomplete="off">
      </div>
      <button type="submit" class="cb-btn danger">Queue change</button>
    </form>
  </section>

  <section class="cb-card">
    <h3 class="cb-card-title">Certify provider capability</h3>
    <p class="cb-card-sub">
      Certification records observed Customer API behaviour. It does not enable
      writes; the separate capability switch remains off until explicitly changed.
    </p>
    <?php if (empty($cb_accounts)): ?>
      <div class="cb-empty">No SecuriAce VPS server account is configured in WHMCS.</div>
    <?php else: ?>
      <form method="post" action="<?= $esc($module_link) ?>" class="cb-stack">
        <input type="hidden" name="action" value="capability-certify">
        <?= generate_token() ?>
        <div class="cb-form-grid">
          <div class="cb-field">
            <label for="cb-cap-account">Provider account</label>
            <select id="cb-cap-account" name="provider_account_id" required>
              <?php foreach ($cb_accounts as $accountId => $account): ?>
                <option value="<?= $esc($accountId) ?>">
                  <?= $esc($account['name'] ?? '') ?> · <?= $esc(substr((string) $accountId, 0, 12)) ?>…
                </option>
              <?php endforeach; ?>
            </select>
          </div>
          <div class="cb-field">
            <label for="cb-cap-name">Capability</label>
            <select id="cb-cap-name" name="capability" required>
              <?php foreach (['create','inspect','start','stop','restart','suspend','unsuspend','terminate','reset_password','reinstall','console','snapshots','reverse_dns','change_package'] as $capability): ?>
                <option value="<?= $esc($capability) ?>"><?= $esc(str_replace('_', ' ', $capability)) ?></option>
              <?php endforeach; ?>
            </select>
          </div>
          <div class="cb-field">
            <label for="cb-cap-state">State</label>
            <select id="cb-cap-state" name="state" required>
              <?php foreach (['not_certified','read_only','requires_manual_action','unsupported','requires_polling','supported'] as $state): ?>
                <option value="<?= $esc($state) ?>"><?= $esc(str_replace('_', ' ', $state)) ?></option>
              <?php endforeach; ?>
            </select>
          </div>
          <div class="cb-field">
            <label for="cb-cap-version">API / certification version</label>
            <input type="text" id="cb-cap-version" name="certification_version"
                   maxlength="120" placeholder="customer-api-YYYY-MM">
          </div>
          <div class="cb-field">
            <label for="cb-cap-evidence">Evidence reference</label>
            <input type="text" id="cb-cap-evidence" name="evidence_reference"
                   maxlength="255" placeholder="Test case, ticket or redacted log reference">
          </div>
          <div class="cb-field">
            <label for="cb-cap-confirmation">For write-capable states, type CERTIFY CAPABILITY</label>
            <input type="text" id="cb-cap-confirmation" name="confirmation"
                   autocomplete="off">
          </div>
        </div>
        <div class="cb-field">
          <label for="cb-cap-notes">Observed semantics and limitations</label>
          <textarea id="cb-cap-notes" name="evidence_notes" rows="3"></textarea>
        </div>
        <button type="submit" class="cb-btn">Record certification</button>
      </form>
    <?php endif; ?>
  </section>
</div>

<div class="cb-workbench-grid">
  <section class="cb-card">
    <header class="cb-section-header">
      <div><h3 class="cb-card-title">Billing compensation</h3><p class="cb-card-sub">Commercial/provider saga state without automatic destructive compensation.</p></div>
      <span class="cb-pill grey"><?= count($cb_billing_sagas) ?> recent</span>
    </header>
    <?php if (empty($cb_billing_sagas)): ?>
      <div class="cb-empty">No provisioning billing saga has been recorded.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_billing_sagas as $saga): ?>
        <li>
          <div>
            <strong>Service #<?= (int) ($saga['service_id'] ?? 0) ?> · <?= $esc(str_replace('_', ' ', (string) ($saga['saga_type'] ?? ''))) ?></strong>
            <div class="muted"><?= $esc($saga['currency'] ?? '') ?> <?= $esc($saga['amount'] ?? '') ?> · <?= $esc($saga['compensation_state'] ?? 'none') ?></div>
          </div>
          <span class="cb-pill <?= $esc($cb_state_tone((string) ($saga['state'] ?? ''))) ?>"><?= $esc($saga['state'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>

  <section class="cb-card">
    <header class="cb-section-header">
      <div><h3 class="cb-card-title">Customer communications</h3><p class="cb-card-sub">WHMCS product-email delivery state. No message body or credential is retained here.</p></div>
      <span class="cb-pill <?= count(array_filter($cb_communications, static function (array $row): bool { return ($row['state'] ?? '') === 'failed'; })) > 0 ? 'warn' : 'grey' ?>"><?= count($cb_communications) ?> recent</span>
    </header>
    <?php if (empty($cb_communications)): ?>
      <div class="cb-empty">No lifecycle communication has been queued.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_communications as $communication): ?>
        <li>
          <div>
            <strong>Service #<?= (int) ($communication['service_id'] ?? 0) ?> · <?= $esc(str_replace('_', ' ', (string) ($communication['message_type'] ?? ''))) ?></strong>
            <div class="muted"><?= $esc($communication['template_name'] ?? '') ?> · attempts <?= (int) ($communication['attempt_count'] ?? 0) ?></div>
          </div>
          <span class="cb-pill <?= $esc($cb_state_tone((string) ($communication['state'] ?? ''))) ?>"><?= $esc($communication['state'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>
</div>

<section class="cb-card">
  <header class="cb-section-header">
    <div>
      <h3 class="cb-card-title">Capability registry</h3>
      <p class="cb-card-sub">Certified state and independent mutation kill switch.</p>
    </div>
    <span class="cb-pill grey"><?= count($cb_capabilities) ?> records</span>
  </header>
  <?php if (empty($cb_capabilities)): ?>
    <div class="cb-empty">No capabilities are certified. All provider mutations fail closed.</div>
  <?php else: ?>
    <div class="cb-table-scroll">
      <table class="cb-table">
        <thead><tr><th>Account</th><th>Capability</th><th>Certification</th><th>Evidence version</th><th>Write switch</th><th>Control</th></tr></thead>
        <tbody>
        <?php foreach ($cb_capabilities as $capability):
          $capName = (string) ($capability['capability'] ?? '');
          $capState = (string) ($capability['state'] ?? 'not_certified');
          $switchEnabled = !empty($cb_capability_switches[$capName]);
        ?>
          <tr>
            <td class="mono"><?= $esc(substr((string) ($capability['provider_account_id'] ?? ''), 0, 12)) ?>…</td>
            <td><?= $esc(str_replace('_', ' ', $capName)) ?></td>
            <td><span class="cb-pill <?= $esc($cb_state_tone($capState)) ?>"><?= $esc($capState) ?></span></td>
            <td class="mono"><?= $esc($capability['certification_version'] ?? '—') ?></td>
            <td><span class="cb-pill <?= $switchEnabled ? 'warn' : 'good' ?>"><?= $switchEnabled ? 'enabled' : 'stopped' ?></span></td>
            <td>
              <form method="post" action="<?= $esc($module_link) ?>" class="cb-inline-form">
                <input type="hidden" name="action" value="provider-write-control">
                <input type="hidden" name="scope" value="capability">
                <input type="hidden" name="capability" value="<?= $esc($capName) ?>">
                <input type="hidden" name="provider_account_id"
                       value="<?= $esc($capability['provider_account_id'] ?? '') ?>">
                <?= generate_token() ?>
                <label class="cb-check compact">
                  <input type="checkbox" name="enabled" value="1"<?= $switchEnabled ? ' checked' : '' ?>>
                  <span>Enabled</span>
                </label>
                <input type="text" name="confirmation" autocomplete="off"
                       aria-label="Typed confirmation"
                       placeholder="ENABLE CAPABILITY WRITE">
                <button type="submit" class="cb-btn subtle">Queue</button>
              </form>
            </td>
          </tr>
        <?php endforeach; ?>
        </tbody>
      </table>
    </div>
  <?php endif; ?>
</section>

<section class="cb-card">
  <header class="cb-section-header">
    <div>
      <h3 class="cb-card-title">Durable operation queue</h3>
      <p class="cb-card-sub">Stable correlation references and explicit recovery actions.</p>
    </div>
    <span class="cb-pill grey"><?= count($cb_operations) ?> on this page</span>
  </header>
  <?php if (empty($cb_operations)): ?>
    <div class="cb-empty">No VPS operations have been created.</div>
  <?php else: ?>
    <div class="cb-table-scroll">
      <table class="cb-table">
        <thead>
          <tr><th>ID / service</th><th>Operation</th><th>State</th><th>Resource truth</th><th>Attempts</th><th>Correlation</th><th>Recovery</th></tr>
        </thead>
        <tbody>
        <?php foreach ($cb_operations as $operation):
          $state = (string) ($operation['state'] ?? '');
          $service = is_array($operation['service'] ?? null) ? $operation['service'] : [];
          $resource = is_array($operation['resource'] ?? null) ? $operation['resource'] : [];
          $operationUuid = (string) ($operation['operation_uuid'] ?? '');
        ?>
          <tr>
            <td>
              <a href="<?= $esc($module_link) ?>&amp;action=operation-detail&amp;operation_uuid=<?= $esc(rawurlencode($operationUuid)) ?>">
                #<?= (int) ($operation['id'] ?? 0) ?>
              </a>
              <div class="muted">Service #<?= (int) ($operation['service_id'] ?? 0) ?> · <?= $esc($service['domainstatus'] ?? 'missing') ?></div>
            </td>
            <td><?= $esc(str_replace('_', ' ', (string) ($operation['operation_type'] ?? ''))) ?></td>
            <td>
              <span class="cb-pill <?= $esc($cb_state_tone($state)) ?>"><?= $esc($state) ?></span>
              <?php if (!empty($operation['safe_error_code'])): ?>
                <div class="mono muted"><?= $esc($operation['safe_error_code']) ?></div>
              <?php endif; ?>
            </td>
            <td>
              <div><?= $esc($resource['provider_state'] ?? 'unknown') ?></div>
              <div class="muted"><?= $esc($resource['provisioning_state'] ?? 'not recorded') ?></div>
            </td>
            <td><?= (int) ($operation['attempt_timeline_count'] ?? 0) ?> / <?= (int) ($operation['max_attempts'] ?? 0) ?></td>
            <td class="mono"><?= $esc(substr((string) ($operation['correlation_id'] ?? ''), 0, 12)) ?>…</td>
            <td>
              <div class="cb-recovery-actions">
              <?php foreach (['reconcile_operation' => 'Reconcile', 'retry_operation' => 'Safe retry', 'cancel_operation' => 'Cancel intent'] as $command => $label): ?>
                <form method="post" action="<?= $esc($module_link) ?>">
                  <input type="hidden" name="action" value="operation-command">
                  <input type="hidden" name="command_type" value="<?= $esc($command) ?>">
                  <input type="hidden" name="operation_uuid" value="<?= $esc($operationUuid) ?>">
                  <?= generate_token() ?>
                  <button type="submit" class="cb-btn ghost"><?= $esc($label) ?></button>
                </form>
              <?php endforeach; ?>
              </div>
            </td>
          </tr>
        <?php endforeach; ?>
        </tbody>
      </table>
    </div>
    <?php if (!empty($next_cursor)): ?>
      <div class="cb-pagination">
        <a class="cb-btn subtle" href="<?= $esc($module_link) ?>&amp;action=operations&amp;before_id=<?= (int) $next_cursor ?>">Older operations</a>
      </div>
    <?php endif; ?>
  <?php endif; ?>
</section>

<div class="cb-workbench-grid">
  <section class="cb-card">
    <header class="cb-section-header">
      <div><h3 class="cb-card-title">Reconciliation findings</h3><p class="cb-card-sub">Open drift and orphan-risk evidence.</p></div>
      <span class="cb-pill <?= empty($cb_findings) ? 'good' : 'warn' ?>"><?= count($cb_findings) ?> open</span>
    </header>
    <?php if (empty($cb_findings)): ?>
      <div class="cb-empty">No open reconciliation findings.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_findings as $finding): ?>
        <li>
          <div>
            <strong><?= $esc(str_replace('_', ' ', (string) ($finding['finding_type'] ?? 'finding'))) ?></strong>
            <div class="muted">Service #<?= (int) ($finding['service_id'] ?? 0) ?> · <?= $esc($finding['safe_next_action'] ?? 'operator review') ?></div>
          </div>
          <span class="cb-pill <?= $esc($cb_state_tone((string) ($finding['severity'] ?? ''))) ?>"><?= $esc($finding['severity'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>

  <section class="cb-card">
    <header class="cb-section-header">
      <div><h3 class="cb-card-title">Existing-service adoption</h3><p class="cb-card-sub">Ownership confidence before destructive self-service.</p></div>
      <span class="cb-pill grey"><?= count($cb_adoption) ?> assessed</span>
    </header>
    <?php if (empty($cb_adoption)): ?>
      <div class="cb-empty">No services have completed adoption assessment.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_adoption as $record):
        $adoptionState = (string) ($record['state'] ?? '');
      ?>
        <li>
          <div>
            <strong>Service #<?= (int) ($record['service_id'] ?? 0) ?></strong>
            <div class="muted">Confidence <?= $esc((string) ($record['confidence'] ?? '0')) ?></div>
            <?php if ($adoptionState === 'probable'): ?>
              <form method="post" action="<?= $esc($module_link) ?>" class="cb-inline-form">
                <input type="hidden" name="action" value="adoption-approve">
                <input type="hidden" name="service_id" value="<?= (int) ($record['service_id'] ?? 0) ?>">
                <input type="hidden" name="provider_resource_id"
                       value="<?= $esc($record['provider_resource_id'] ?? '') ?>">
                <input type="hidden" name="evidence_hash"
                       value="<?= $esc(hash('sha256', (string) ($record['evidence_json'] ?? ''))) ?>">
                <?= generate_token() ?>
                <label>
                  <span class="sr-only">Type VERIFY OWNERSHIP to approve this candidate</span>
                  <input type="text" name="confirmation" autocomplete="off"
                         placeholder="VERIFY OWNERSHIP" required>
                </label>
                <button type="submit" class="cb-btn subtle">Verify candidate</button>
              </form>
            <?php endif; ?>
          </div>
          <span class="cb-pill <?= $esc($cb_state_tone($adoptionState)) ?>"><?= $esc($adoptionState) ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>
</div>

<section class="cb-card">
  <header class="cb-section-header">
    <div><h3 class="cb-card-title">Operator command ledger</h3><p class="cb-card-sub">Addon requests and provisioning-worker validation outcomes.</p></div>
    <span class="cb-pill grey"><?= count($cb_commands) ?> recent</span>
  </header>
  <?php if (empty($cb_commands)): ?>
    <div class="cb-empty">No recovery or write-control commands have been requested.</div>
  <?php else: ?>
    <div class="cb-table-scroll">
      <table class="cb-table">
        <thead><tr><th>Command</th><th>Scope</th><th>Requested by</th><th>State</th><th>Safe result</th><th>Created</th></tr></thead>
        <tbody>
        <?php foreach ($cb_commands as $command):
          $commandState = (string) ($command['state'] ?? '');
        ?>
          <tr>
            <td><?= $esc(str_replace('_', ' ', (string) ($command['command_type'] ?? ''))) ?></td>
            <td class="mono"><?= !empty($command['operation_uuid']) ? $esc(substr((string) $command['operation_uuid'], 0, 12)) . '…' : 'global' ?></td>
            <td>Admin #<?= (int) ($command['requested_by_admin_id'] ?? 0) ?></td>
            <td><span class="cb-pill <?= $esc($cb_state_tone($commandState)) ?>"><?= $esc($commandState) ?></span></td>
            <td class="mono"><?= $esc($command['safe_error_code'] ?? '—') ?></td>
            <td class="mono"><?= $esc(substr((string) ($command['created_at'] ?? ''), 0, 16)) ?></td>
          </tr>
        <?php endforeach; ?>
        </tbody>
      </table>
    </div>
  <?php endif; ?>
</section>

</div>
