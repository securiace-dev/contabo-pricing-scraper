<?php
/**
 * Legacy hook loader. The canonical hook file uses require_once semantics so
 * installations that temporarily contain both module names register one
 * worker only.
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

require_once dirname(__DIR__) . '/securiacevps/hooks.php';
