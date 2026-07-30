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
    <div class="sav-notice sav-notice--{$flash_tone|escape}"
         role="status" aria-live="polite">
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
                         autocomplete="off" spellcheck="false" required>
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

  <section class="sav-card sav-snapshots" aria-labelledby="sav-snapshots-title">
    <div class="sav-card-head">
      <div>
        <span class="sav-status-label">Recovery points</span>
        <h3 id="sav-snapshots-title">Snapshots</h3>
      </div>
      {if $snapshot_list_certified}
        <span class="sav-snapshot-count">
          {$snapshots|@count} observed
        </span>
      {/if}
    </div>

    {if !$snapshot_list_certified}
      <div class="sav-empty">
        Snapshot management is not certified for this provider account.
      </div>
    {else}
      {if $snapshot_actions.create}
        <details class="sav-snapshot-create">
          <summary class="sav-btn sav-btn--secondary">Create snapshot</summary>
          <form method="post" class="sav-snapshot-form">
            {$csrf_field nofilter}
            <input type="hidden" name="securiacevps_action" value="snapshot_create">
            <label for="sav-snapshot-name">Name</label>
            <input id="sav-snapshot-name" type="text" name="snapshot_name"
                   minlength="1" maxlength="30"
                   pattern="[A-Za-z0-9 -]+"
                   autocomplete="off" spellcheck="false" required>
            <label for="sav-snapshot-description">Description <span>(optional)</span></label>
            <textarea id="sav-snapshot-description" name="snapshot_description"
                      maxlength="255" rows="3" autocomplete="off"></textarea>
            <button type="submit" class="sav-btn sav-btn--primary">Create snapshot</button>
          </form>
        </details>
      {/if}

      {if $snapshots}
        <div class="sav-snapshot-list">
          {foreach from=$snapshots item=snapshot}
            <article class="sav-snapshot">
              <div class="sav-snapshot-copy">
                <strong>{$snapshot.name|escape}</strong>
                {if $snapshot.description}
                  <p>{$snapshot.description|escape}</p>
                {/if}
                <dl>
                  <div>
                    <dt>Created</dt>
                    <dd>{if $snapshot.provider_created_at}{$snapshot.provider_created_at|escape}{else}Unknown{/if}</dd>
                  </div>
                  <div>
                    <dt>Automatic deletion</dt>
                    <dd>{if $snapshot.provider_auto_delete_at}{$snapshot.provider_auto_delete_at|escape}{else}Not reported{/if}</dd>
                  </div>
                  <div>
                    <dt>Image</dt>
                    <dd>{if $snapshot.image_name}{$snapshot.image_name|escape}{else}{$snapshot.image_id|escape}{/if}</dd>
                  </div>
                </dl>
              </div>

              {if $snapshot_actions.delete || $snapshot_actions.rollback}
                <div class="sav-snapshot-actions">
                  {if $snapshot_actions.rollback}
                    <details class="sav-confirm">
                      <summary class="sav-btn sav-btn--warning">Roll back</summary>
                      <form method="post" class="sav-confirm-body">
                        {$csrf_field nofilter}
                        <input type="hidden" name="securiacevps_action" value="snapshot_rollback">
                        <input type="hidden" name="snapshot_id" value="{$snapshot.snapshot_id|escape}">
                        <input type="hidden" name="snapshot_evidence" value="{$snapshot.payload_hash|escape}">
                        <p class="sav-risk-copy">
                          This restores the server and Contabo will automatically delete
                          every newer snapshot.
                        </p>
                        <label for="sav-rollback-{$snapshot@iteration}">
                          Type <code>ROLL BACK SNAPSHOT</code>
                        </label>
                        <input id="sav-rollback-{$snapshot@iteration}" type="text"
                               name="confirmation" autocomplete="off"
                               spellcheck="false" required>
                        <button type="submit" class="sav-btn sav-btn--warning">
                          Roll back to this snapshot
                        </button>
                      </form>
                    </details>
                  {/if}

                  {if $snapshot_actions.delete}
                    <details class="sav-confirm">
                      <summary class="sav-btn sav-btn--danger">Delete</summary>
                      <form method="post" class="sav-confirm-body">
                        {$csrf_field nofilter}
                        <input type="hidden" name="securiacevps_action" value="snapshot_delete">
                        <input type="hidden" name="snapshot_id" value="{$snapshot.snapshot_id|escape}">
                        <input type="hidden" name="snapshot_evidence" value="{$snapshot.payload_hash|escape}">
                        <label for="sav-delete-{$snapshot@iteration}">
                          Type <code>DELETE SNAPSHOT</code>
                        </label>
                        <input id="sav-delete-{$snapshot@iteration}" type="text"
                               name="confirmation" autocomplete="off"
                               spellcheck="false" required>
                        <button type="submit" class="sav-btn sav-btn--danger">
                          Delete snapshot
                        </button>
                      </form>
                    </details>
                  {/if}
                </div>
              {/if}
            </article>
          {/foreach}
        </div>
        <p class="sav-observation-note">
          Snapshot inventory is provider-observed and updates only after Refresh details
          or a completed snapshot operation.
        </p>
      {else}
        <div class="sav-empty">
          No snapshots were present at the last successful provider observation.
        </div>
      {/if}
    {/if}
  </section>
</section>
