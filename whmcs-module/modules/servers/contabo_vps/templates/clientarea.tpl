{* Contabo VPS — client area panel. Rendered by contabo_vps_ClientArea(). *}
{if $error}
  <div class="alert alert-danger">{$error|escape}</div>
{else}
  <div class="row">
    <div class="col-sm-12">
      <table class="table table-striped">
        <tbody>
          <tr><th width="30%">Instance ID</th><td>{$instance_id|escape}</td></tr>
          <tr><th>Status</th><td>{$status|escape}</td></tr>
          <tr><th>Region</th><td>{$region|escape}</td></tr>
          <tr><th>Image</th><td>{$image|escape}</td></tr>
        </tbody>
      </table>
      <p class="text-muted">
        Power actions (start / stop / restart) are available from your provider on request.
      </p>
    </div>
  </div>
{/if}
