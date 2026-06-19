#!/usr/bin/env bash
#
# local-whmcs.sh — install + test the contabo_pricing addon against the local
# dockerised WHMCS dev stack from the securiace-vps-platform project.
#
# That stack (docker-compose.test-whmcs.yml) runs two parallel WHMCS instances:
#   http://localhost:8013/  — WHMCS 8.13  (container securiace-vps-platform-whmcs8-1   / -php-1)
#   http://localhost:8090/  — WHMCS 9.0   (container securiace-vps-platform-whmcs9-1   / -php-1)
# WHMCS source is bind-mounted from:
#   <platform>/deploy/whmcs-test/source/8.13/  and  /9.0/
# so dropping our addon into source/<v>/modules/addons/ makes the running
# container see it immediately (no docker cp needed for the PHP files).
#
# This script lives in the contabo-pricing-scraper project (the addon's home).
# It only WRITES into the platform's gitignored WHMCS source tree + runs
# docker exec against the WHMCS php containers. It never touches the platform's
# own code, and never touches production (my.securiace.com).
#
# Usage:
#   scripts/local-whmcs.sh sync        # rsync addon into both 8.13 + 9.0 source
#   scripts/local-whmcs.sh migrate 8   # run Installer::upgrade in the 8.13 php container
#   scripts/local-whmcs.sh migrate 9   # run Installer::upgrade in the 9.0  php container
#   scripts/local-whmcs.sh activate 8  # insert tbladdonmodules rows + run activate hook
#   scripts/local-whmcs.sh render 8 dashboard   # SSH-render-equivalent: dump a page's HTML
#   scripts/local-whmcs.sh status      # show containers + addon presence
set -euo pipefail

PLATFORM="${SECVPS_PLATFORM_DIR:-$HOME/Projects/securiace-vps-platform}"
ADDON_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/whmcs-module/modules/addons/contabo_pricing"

src_dir()  { echo "$PLATFORM/deploy/whmcs-test/source/$1"; }          # $1 = 8.13 | 9.0
php_ctr()  { case "$1" in 8) echo "securiace-vps-platform-whmcs8-php-1";; 9) echo "securiace-vps-platform-whmcs9-php-1";; esac; }
ver_dir()  { case "$1" in 8) echo "8.13";; 9) echo "9.0";; esac; }

require_platform() {
  [ -d "$PLATFORM" ] || { echo "ERROR: securiace-vps-platform dir not found at: $PLATFORM" >&2;
    echo "       set SECVPS_PLATFORM_DIR to its location." >&2; exit 2; }
  command -v rsync >/dev/null 2>&1 || { echo "ERROR: rsync not found on PATH" >&2; exit 2; }
}
require_docker() {
  command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found on PATH" >&2; exit 2; }
}

sync_one() {
  local v="$1" dest; dest="$(src_dir "$(ver_dir "$v")")/modules/addons/contabo_pricing"
  rsync -a --delete \
    --exclude vendor/ --exclude tests/ --exclude phpunit.xml \
    --exclude '.phpunit.cache' --exclude composer.lock --exclude '.git*' \
    "$ADDON_SRC/" "$dest/"
  echo "  synced → $dest"
}

cmd="${1:-status}"
case "$cmd" in
  sync)
    require_platform
    echo "==> syncing addon into local WHMCS source (8.13 + 9.0)"
    sync_one 8; sync_one 9
    ;;
  migrate)
    require_docker
    v="${2:-8}"; ctr="$(php_ctr "$v")"
    echo "==> migrate (Installer::upgrade) in WHMCS $(ver_dir "$v") [$ctr]"
    docker exec -i "$ctr" php -r '
      chdir("/var/www/html");
      require "init.php";
      spl_autoload_register(function($c){ if(strpos($c,"ContaboPricing\\")===0){ $p="/var/www/html/modules/addons/contabo_pricing/lib/".str_replace(["ContaboPricing\\","\\"],["","/"],$c).".php"; if(is_file($p)) require_once $p; }});
      $r = \ContaboPricing\SchemaHealth::assertOrMigrate();
      echo json_encode($r).PHP_EOL;
      $h = \ContaboPricing\SchemaHealth::requiredColumnsPresent();
      echo "health: ".json_encode($h).PHP_EOL;
    '
    ;;
  activate)
    require_docker
    v="${2:-8}"; ctr="$(php_ctr "$v")"
    echo "==> activate addon (config rows + activate hook) in WHMCS $(ver_dir "$v")"
    docker exec -i "$ctr" php -r '
      chdir("/var/www/html");
      require "init.php";
      use WHMCS\Database\Capsule;
      require "modules/addons/contabo_pricing/contabo_pricing.php";
      // seed config rows so Settings::fromVars has values
      $cfg = ["api_base_url"=>"http://127.0.0.1:8080/api/v1","api_token"=>"localtest","currency_iso"=>"INR","apply_gst_18"=>"yes","fx_markup_pct"=>"3.5","default_sync_strategy"=>"manual","log_retention_days"=>"365"];
      foreach ($cfg as $k=>$v){ Capsule::table("tbladdonmodules")->updateOrInsert(["module"=>"contabo_pricing","setting"=>$k],["value"=>$v]); }
      $r = contabo_pricing_activate();
      echo json_encode($r).PHP_EOL;
    '
    ;;
  render)
    require_docker
    v="${2:-8}"; ctr="$(php_ctr "$v")"; action="${3:-dashboard}"
    docker exec -e ACTION="$action" -i "$ctr" php -r '
      chdir("/var/www/html");
      session_start(); $_SESSION["adminid"]=1; $_SESSION["uid"]=1;
      $_SERVER["REQUEST_METHOD"]="GET"; $_SERVER["HTTP_HOST"]="localhost"; $_SERVER["REQUEST_URI"]="/admin/addonmodules.php"; $_SERVER["SCRIPT_NAME"]="/admin/addonmodules.php";
      require "init.php";
      require "modules/addons/contabo_pricing/contabo_pricing.php";
      $rows = \WHMCS\Database\Capsule::table("tbladdonmodules")->where("module","contabo_pricing")->get(["setting","value"]);
      $vars=["modulelink"=>"addonmodules.php?module=contabo_pricing"]; foreach($rows as $r){ $vars[(string)$r->setting]=(string)$r->value; }
      $_REQUEST["action"]=getenv("ACTION")?:"dashboard";
      ob_start(); contabo_pricing_output($vars); echo ob_get_clean();
    '
    ;;
  status)
    require_docker
    echo "==> containers"; docker ps --format '{{.Names}}\t{{.Status}}' | grep -iE "securiace-vps-platform-whmcs|mariadb" || true
    echo "==> addon present in source?"
    for v in 8.13 9.0; do d="$(src_dir "$v")/modules/addons/contabo_pricing"; echo "  $v: $([ -d "$d" ] && echo present || echo absent)"; done
    ;;
  *) echo "unknown command: $cmd"; exit 2;;
esac
