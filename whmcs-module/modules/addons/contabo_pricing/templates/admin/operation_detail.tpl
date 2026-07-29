<?php require __DIR__ . '/_layout_open.tpl'; ?>
<?php
$cb_operation = is_array($operation ?? null) ? $operation : [];
$cb_service = is_array($service ?? null) ? $service : [];
$cb_resource = is_array($resource ?? null) ? $resource : [];
$cb_attempts = is_array($attempts ?? null) ? $attempts : [];
$cb_provider_requests = is_array($provider_requests ?? null) ? $provider_requests : [];
$cb_commands = is_array($commands ?? null) ? $commands : [];
$cb_billing_sagas = is_array($billing_sagas ?? null) ? $billing_sagas : [];
$cb_communications = is_array($communications ?? null) ? $communications : [];
$cb_events = is_array($audit_events ?? null) ? $audit_events : [];
$cb_findings = is_array($findings ?? null) ? $findings : [];
$cb_uuid = (string) ($cb_operation['operation_uuid'] ?? '');
?>

<header class="cb-page-header">
  <div>
    <a href="<?= $esc($module_link) ?>&amp;action=operations">← VPS operations</a>
    <h2 class="display">Operation timeline</h2>
    <p class="cb-card-sub">
      Customer-safe evidence for service #<?= (int) ($cb_operation['service_id'] ?? 0) ?>.
      Provider response bodies and secrets are intentionally not rendered.
    </p>
  </div>
  <span class="cb-pill grey"><?= $esc($cb_operation['state'] ?? 'unknown') ?></span>
</header>

<section class="cb-card">
  <h3 class="cb-card-title">Identity and current truth</h3>
  <dl class="cb-key-values">
    <div><dt>Operation UUID</dt><dd class="mono"><?= $esc($cb_uuid) ?></dd></div>
    <div><dt>Command ID</dt><dd class="mono"><?= $esc($cb_operation['command_id'] ?? '') ?></dd></div>
    <div><dt>Correlation ID</dt><dd class="mono"><?= $esc($cb_operation['correlation_id'] ?? '') ?></dd></div>
    <div><dt>Type</dt><dd><?= $esc(str_replace('_', ' ', (string) ($cb_operation['operation_type'] ?? ''))) ?></dd></div>
    <div><dt>WHMCS state</dt><dd><?= $esc($cb_service['domainstatus'] ?? 'service missing') ?></dd></div>
    <div><dt>Provisioning state</dt><dd><?= $esc($cb_resource['provisioning_state'] ?? 'not recorded') ?></dd></div>
    <div><dt>Provider state</dt><dd><?= $esc($cb_resource['provider_state'] ?? 'unknown') ?></dd></div>
    <div><dt>Ownership state</dt><dd><?= $esc($cb_resource['ownership_state'] ?? 'unverified') ?></dd></div>
    <div><dt>Safe error</dt><dd class="mono"><?= $esc($cb_operation['safe_error_code'] ?? '—') ?></dd></div>
    <div><dt>Retry class</dt><dd><?= $esc($cb_operation['retry_classification'] ?? '—') ?></dd></div>
  </dl>
</section>

<section class="cb-card">
  <h3 class="cb-card-title">Attempt timeline</h3>
  <?php if (empty($cb_attempts)): ?>
    <div class="cb-empty">No worker attempt has started.</div>
  <?php else: ?>
    <ol class="cb-timeline">
    <?php foreach ($cb_attempts as $attempt): ?>
      <li>
        <div class="cb-timeline-marker"></div>
        <div>
          <div class="cb-section-header">
            <strong>Attempt <?= (int) ($attempt['attempt_number'] ?? 0) ?></strong>
            <span class="cb-pill grey"><?= $esc($attempt['state'] ?? '') ?></span>
          </div>
          <dl class="cb-key-values compact">
            <div><dt>Fencing token</dt><dd class="mono"><?= (int) ($attempt['fencing_token'] ?? 0) ?></dd></div>
            <div><dt>Provider request</dt><dd class="mono"><?= $esc($attempt['provider_request_id'] ?? 'not returned') ?></dd></div>
            <div><dt>Safe error</dt><dd class="mono"><?= $esc($attempt['safe_error_code'] ?? '—') ?></dd></div>
            <div><dt>Started</dt><dd class="mono"><?= $esc($attempt['started_at'] ?? '') ?></dd></div>
            <div><dt>Finished</dt><dd class="mono"><?= $esc($attempt['finished_at'] ?? 'running') ?></dd></div>
          </dl>
        </div>
      </li>
    <?php endforeach; ?>
    </ol>
  <?php endif; ?>
</section>

<div class="cb-workbench-grid">
  <section class="cb-card">
    <h3 class="cb-card-title">Provider request markers</h3>
    <?php if (empty($cb_provider_requests)): ?>
      <div class="cb-empty">No provider mutation has been recorded.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_provider_requests as $request): ?>
        <li>
          <div>
            <strong><?= $esc($request['state'] ?? '') ?></strong>
            <div class="mono muted"><?= $esc($request['provider_request_id'] ?? 'request ID unavailable') ?></div>
          </div>
          <span class="cb-pill <?= !empty($request['unknown_outcome']) ? 'warn' : 'grey' ?>">
            <?= !empty($request['unknown_outcome']) ? 'unknown outcome' : 'recorded' ?>
          </span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>

  <section class="cb-card">
    <h3 class="cb-card-title">Recovery commands</h3>
    <?php if (empty($cb_commands)): ?>
      <div class="cb-empty">No operator command has been requested.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_commands as $command): ?>
        <li>
          <div>
            <strong><?= $esc(str_replace('_', ' ', (string) ($command['command_type'] ?? ''))) ?></strong>
            <div class="muted">Admin #<?= (int) ($command['requested_by_admin_id'] ?? 0) ?> · <?= $esc($command['created_at'] ?? '') ?></div>
          </div>
          <span class="cb-pill grey"><?= $esc($command['state'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>
</div>

<div class="cb-workbench-grid">
  <section class="cb-card">
    <h3 class="cb-card-title">Billing saga</h3>
    <?php if (empty($cb_billing_sagas)): ?>
      <div class="cb-empty">No billing compensation record is linked to this operation.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_billing_sagas as $saga): ?>
        <li>
          <div>
            <strong><?= $esc(str_replace('_', ' ', (string) ($saga['saga_type'] ?? ''))) ?></strong>
            <div class="muted"><?= $esc($saga['currency'] ?? '') ?> <?= $esc($saga['amount'] ?? '') ?> · <?= $esc($saga['compensation_state'] ?? 'none') ?></div>
          </div>
          <span class="cb-pill grey"><?= $esc($saga['state'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>

  <section class="cb-card">
    <h3 class="cb-card-title">Customer communication</h3>
    <?php if (empty($cb_communications)): ?>
      <div class="cb-empty">No lifecycle communication is linked to this operation.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_communications as $communication): ?>
        <li>
          <div>
            <strong><?= $esc(str_replace('_', ' ', (string) ($communication['message_type'] ?? ''))) ?></strong>
            <div class="muted"><?= $esc($communication['template_name'] ?? '') ?> · attempts <?= (int) ($communication['attempt_count'] ?? 0) ?></div>
          </div>
          <span class="cb-pill grey"><?= $esc($communication['state'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>
</div>

<div class="cb-workbench-grid">
  <section class="cb-card">
    <h3 class="cb-card-title">Audit events</h3>
    <?php if (empty($cb_events)): ?>
      <div class="cb-empty">No audit event is recorded for this service.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_events as $event): ?>
        <li>
          <div>
            <strong><?= $esc(str_replace('_', ' ', (string) ($event['event_type'] ?? ''))) ?></strong>
            <div class="muted"><?= $esc($event['created_at'] ?? '') ?> · actor <?= $esc($event['actor_type'] ?? '') ?></div>
          </div>
          <span class="cb-pill grey"><?= $esc($event['outcome'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>

  <section class="cb-card">
    <h3 class="cb-card-title">Reconciliation evidence</h3>
    <?php if (empty($cb_findings)): ?>
      <div class="cb-empty">No finding is linked to this service.</div>
    <?php else: ?>
      <ul class="cb-record-list">
      <?php foreach ($cb_findings as $finding): ?>
        <li>
          <div>
            <strong><?= $esc(str_replace('_', ' ', (string) ($finding['finding_type'] ?? ''))) ?></strong>
            <div class="muted"><?= $esc($finding['safe_next_action'] ?? 'operator review') ?></div>
          </div>
          <span class="cb-pill grey"><?= $esc($finding['state'] ?? '') ?></span>
        </li>
      <?php endforeach; ?>
      </ul>
    <?php endif; ?>
  </section>
</div>

</div>
