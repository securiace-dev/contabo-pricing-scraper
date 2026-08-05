'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const taxonomy = require('../scripts/contabo_scraper');

test('Node scraper exposes the current VPS taxonomy with legacy-compatible aliases', () => {
  assert.equal(taxonomy.canonicalFamilyFromProduct({ type: 'vps', slug: 'cloud-vps-core-4' }), 'Core VPS');
  assert.equal(taxonomy.canonicalFamilyFromProduct({ type: 'vps', slug: 'cloud-vps-plus-4' }), 'Performance VPS');
  assert.equal(taxonomy.canonicalFamilyFromProduct({ type: 'vps', slug: 'cloud-vps-10' }), 'Cloud VPS');
  assert.equal(taxonomy.canonicalFamilyFromProduct({ type: 'vds', slug: 'vds-s' }), 'Max Performance VPS');
  assert.equal(taxonomy.familyFromProduct({ type: 'vds' }), 'Cloud VDS');
});

test('storage policy is explicit for new categories', () => {
  assert.equal(taxonomy.storagePolicyFor('Core VPS'), 'ssd_only');
  assert.equal(taxonomy.storagePolicyFor('Performance VPS'), 'nvme_only');
  assert.equal(taxonomy.storagePolicyFor('Max Performance VPS'), 'legacy_vds_options');
  assert.equal(taxonomy.storagePolicyFor('Cloud VPS'), 'legacy_cloud_vps_options');
  assert.equal(taxonomy.titleFromSlug('cloud-vps-core-12'), 'Core VPS 12');
  assert.equal(taxonomy.titleFromSlug('cloud-vps-plus-18'), 'Performance VPS 18');
});

test('region classifier tolerates current location wrappers and localized labels', () => {
  assert.deepEqual(taxonomy.classifyRegion('Location: Australia [Cloud VPS Plus 8]'), {
    region_group: 'Australia', country: 'Australia', country_code: 'AU',
  });
  assert.deepEqual(taxonomy.classifyRegion('Vereinigte Staaten (Central)'), {
    region_group: 'America', country: 'United States (Central)', country_code: 'US',
    subregion: 'Central',
  });
});

test('fragment URLs address object-storage products consistently with Rust', () => {
  assert.equal(taxonomy.slugFromUrl('https://contabo.com/en/object-storage/#european-union'), 'european-union');
  assert.equal(taxonomy.slugFromUrl('https://contabo.com/en/object-storage/#singapore'), 'singapore');
});
