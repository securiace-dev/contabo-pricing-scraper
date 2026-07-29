{* Contabo VPS — client area panel. Rendered by securiacevps_ClientArea().
   Core Smarty only (Smarty 3 on WHMCS 8, Smarty 4/5 on WHMCS 9). Power and
   password actions are rendered by WHMCS itself from
   ClientAreaCustomButtonArray in the service sidebar. *}
{if $stale}
  <div class="alert alert-warning">
    Live server status is temporarily unavailable — showing the last synced details.
    <br><small>{$stale_error|escape}</small>
  </div>
{/if}

<div class="row">
  <div class="col-sm-12">
    <table class="table table-striped">
      <tbody>
        {if $instance_id}
          <tr><th style="width:30%">Instance ID</th><td>{$instance_id|escape}</td></tr>
        {/if}
        <tr>
          <th style="width:30%">Status</th>
          <td>
            {if $status == 'running'}
              <span class="label label-success badge badge-success bg-success">{$status|escape}</span>
            {elseif $status == 'stopped'}
              <span class="label label-default badge badge-secondary bg-secondary">{$status|escape}</span>
            {elseif $status == 'provisioning' || $status == 'installing'}
              <span class="label label-info badge badge-info bg-info">{$status|escape}</span> <em>— your server is being built, this usually completes within minutes</em>
            {elseif $status == 'unavailable'}
              <span class="label label-warning badge badge-warning bg-warning">{$status|escape}</span>
            {else}
              <span class="label label-warning badge badge-warning bg-warning">{$status|escape}</span>
            {/if}
          </td>
        </tr>
        <tr>
          <th>IPv4 Address{if $ipv4|@count > 1}es{/if}</th>
          <td>
            {if $ipv4}
              {foreach from=$ipv4 item=ip}<code>{$ip|escape}</code>{if !$ip@last}<br>{/if}{/foreach}
            {else}
              <em>not yet assigned</em>
            {/if}
          </td>
        </tr>
        {if $ipv6}
        <tr>
          <th>IPv6 Address{if $ipv6|@count > 1}es{/if}</th>
          <td>{foreach from=$ipv6 item=ip}<code>{$ip|escape}</code>{if !$ip@last}<br>{/if}{/foreach}</td>
        </tr>
        {/if}
        {if $region}<tr><th>Region</th><td>{$region|escape}</td></tr>{/if}
        {if $image}<tr><th>Image</th><td>{$image|escape}</td></tr>{/if}
        {if $created}<tr><th>Created</th><td>{$created|escape}</td></tr>{/if}
        {if $synced_at}<tr><th>Last Synced</th><td>{$synced_at|escape}</td></tr>{/if}
      </tbody>
    </table>
    <p class="text-muted">
      Use the <strong>Start</strong>, <strong>Stop</strong>, <strong>Restart</strong> and
      <strong>Reset Root Password</strong> actions in the sidebar to manage your server.
      After a password reset, the new root password is shown in this page's service details.
    </p>
  </div>
</div>
