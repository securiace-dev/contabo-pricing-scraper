<link rel="stylesheet"
      href="{$WEB_ROOT}/modules/servers/securiacevps/assets/clientarea.css?v=2.0.0">

<section class="sav-panel" aria-labelledby="sav-title">
  <header class="sav-head">
    <div class="sav-head-copy">
      <h2 id="sav-title">Your server, as it is now</h2>
      <p class="sav-lede">
        Provider status, ownership checks, pending work and recovery controls in one place.
      </p>
    </div>
    <div class="sav-status-block" data-state="{$status|escape}">
      <span class="sav-status-dot" aria-hidden="true"></span>
      <div>
        <span class="sav-status-label">Provider status</span>
        <strong>{$status|replace:'_':' '|escape|capitalize}</strong>
      </div>
    </div>
  </header>

  {if $flash}
    <div class="sav-notice sav-notice--{$flash_tone|escape}" role="status">
      {$flash|escape}
    </div>
  {/if}

  {if $revealed_credential}
    <section class="sav-secret" aria-labelledby="sav-secret-title">
      <div>
        <h3 id="sav-secret-title">Copy the root password now</h3>
        <p>This value has been removed from storage and cannot be displayed again.</p>
      </div>
      <code class="sav-secret-value">{$revealed_credential|escape}</code>
    </section>
  {/if}

  <div class="sav-truth-rail" aria-label="Service truth">
    <div class="sav-truth">
      <span>Provisioning</span>
      <strong>{$provisioning_state|replace:'_':' '|escape|capitalize}</strong>
    </div>
    <div class="sav-truth">
      <span>Ownership</span>
      <strong>{$ownership_state|replace:'_':' '|escape|capitalize}</strong>
    </div>
    <div class="sav-truth">
      <span>Last observed</span>
      <strong>{if $synced_at}{$synced_at|escape}{else}Not yet observed{/if}</strong>
    </div>
  </div>

  {if $operation}
    <section class="sav-operation" aria-labelledby="sav-operation-title">
      <div class="sav-operation-mark" aria-hidden="true"></div>
      <div>
        <span class="sav-status-label">Latest operation</span>
        <h3 id="sav-operation-title">
          {$operation.operation_type|replace:'_':' '|escape|capitalize}
          · {$operation.state|replace:'_':' '|escape|capitalize}
        </h3>
        <p>
          Reference <code>{$operation.correlation_id|escape}</code>
          {if $operation.safe_error_code}
            · {$operation.safe_error_code|replace:'_':' '|escape}
          {/if}
        </p>
      </div>
    </section>
  {/if}

  <div class="sav-main-grid">
    <section class="sav-card" aria-labelledby="sav-details-title">
      <div class="sav-card-head">
        <div>
          <h3 id="sav-details-title">Server details</h3>
        </div>
        <form method="post" class="sav-inline-form">
          {$csrf_field nofilter}
          <input type="hidden" name="securiacevps_action" value="refresh">
          <button type="submit" class="sav-btn sav-btn--quiet">Refresh details</button>
        </form>
      </div>
      <dl class="sav-facts">
        <div>
          <dt>Instance</dt>
          <dd>{if $instance_id}<code>{$instance_id|escape}</code>{else}Awaiting assignment{/if}</dd>
        </div>
        <div>
          <dt>Region</dt>
          <dd>{if $region}{$region|escape}{else}—{/if}</dd>
        </div>
        <div>
          <dt>Image</dt>
          <dd>{if $image}{$image|escape}{else}—{/if}</dd>
        </div>
        <div>
          <dt>IPv4</dt>
          <dd>
            {if $ipv4}
              {foreach from=$ipv4 item=ip}<code>{$ip|escape}</code>{if !$ip@last}<br>{/if}{/foreach}
            {else}
              Not yet assigned
            {/if}
          </dd>
        </div>
        <div>
          <dt>IPv6</dt>
          <dd>
            {if $ipv6}
              {foreach from=$ipv6 item=ip}<code>{$ip|escape}</code>{if !$ip@last}<br>{/if}{/foreach}
            {else}
              Not assigned
            {/if}
          </dd>
        </div>
      </dl>
    </section>

    <section class="sav-card sav-actions-card" aria-labelledby="sav-actions-title">
      <h3 id="sav-actions-title">Available actions</h3>

      {if !$verified_ownership}
        <div class="sav-empty">
          Actions are locked until an administrator verifies that this WHMCS service owns
          the provider resource.
        </div>
      {elseif $busy}
        <div class="sav-empty">
          An operation is already in progress. Controls will return when its outcome is known.
        </div>
      {elseif !$writes_enabled}
        <div class="sav-empty">
          Provider changes are temporarily paused. Read-only service details remain available.
        </div>
      {elseif !$actions}
        <div class="sav-empty">
          No provider actions are certified for this server right now.
        </div>
      {else}
        <div class="sav-action-list">
          {foreach from=$actions key=action item=meta}
            {if $meta.confirmation}
              <details class="sav-confirm">
                <summary class="sav-btn sav-btn--{$meta.tone|escape}">{$meta.label|escape}</summary>
                <form method="post" class="sav-confirm-body">
                  {$csrf_field nofilter}
                  <input type="hidden" name="securiacevps_action" value="{$action|escape}">
                  <label for="sav-confirm-{$action|escape}">
                    Type <code>{$meta.confirmation|escape}</code> to continue
                  </label>
                  <input id="sav-confirm-{$action|escape}" type="text" name="confirmation"
                         autocomplete="off" required>
                  <button type="submit" class="sav-btn sav-btn--{$meta.tone|escape}">
                    {$meta.label|escape}
                  </button>
                </form>
              </details>
            {else}
              <form method="post">
                {$csrf_field nofilter}
                <input type="hidden" name="securiacevps_action" value="{$action|escape}">
                <button type="submit" class="sav-btn sav-btn--{$meta.tone|escape}">
                  {$meta.label|escape}
                </button>
              </form>
            {/if}
          {/foreach}
        </div>
      {/if}

      {if $credential}
        <div class="sav-credential-ready">
          <div>
            <strong>New credential ready</strong>
            <span>Available until {$credential.expires_at|escape}; one reveal only.</span>
          </div>
          <form method="post">
            {$csrf_field nofilter}
            <input type="hidden" name="securiacevps_action" value="reveal_credential">
            <input type="hidden" name="reveal_token" value="{$credential.reveal_token|escape}">
            <button type="submit" class="sav-btn sav-btn--primary">Reveal once</button>
          </form>
        </div>
      {/if}
    </section>
  </div>
</section>
