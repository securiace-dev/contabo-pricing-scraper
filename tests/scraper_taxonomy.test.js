'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const taxonomy = require('../scripts/contabo_scraper');

test('Node and Rust default discovery share the 22-plan active taxonomy', () => {
  assert.equal(taxonomy.ALL_PLAN_URLS.length, 22);
  assert.equal(taxonomy.ALL_PLAN_URLS.filter((url) => url.includes('cloud-vps-core-')).length, 6);
  assert.equal(taxonomy.ALL_PLAN_URLS.filter((url) => url.includes('cloud-vps-plus-')).length, 6);
  assert.equal(taxonomy.ALL_PLAN_URLS.filter((url) => url.includes('/vds/')).length, 5);
  assert.equal(taxonomy.ALL_PLAN_URLS.filter((url) => url.includes('/storage-vps/')).length, 5);
  assert.equal(taxonomy.ALL_PLAN_URLS.some((url) => /cloud-vps-(?:10|20|30|40|50|60)$/.test(url)), false);
});

test('current families retain explicit legacy aliases', () => {
  assert.deepEqual(taxonomy.familyInfoFromProduct({ type: 'vps', slug: 'cloud-vps-core-4' }), {
    canonical: 'Core VPS', aliases: ['Cloud VPS'],
  });
  assert.deepEqual(taxonomy.familyInfoFromProduct({ type: 'vps', slug: 'cloud-vps-plus-4' }), {
    canonical: 'Performance VPS', aliases: ['Cloud VPS Plus', 'Cloud VPS'],
  });
  assert.deepEqual(taxonomy.familyInfoFromProduct({ type: 'vds', slug: 'vds-s' }), {
    canonical: 'Max Performance VPS', aliases: ['Cloud VDS', 'VDS'],
  });
  assert.deepEqual(taxonomy.familyInfoFromProduct({ type: 'storage-vps', slug: 'storage-vps-10' }), {
    canonical: 'Storage VPS', aliases: [],
  });
});

test('storage policy follows the active pricing cards', () => {
  assert.equal(taxonomy.storagePolicyAllows({ type: 'vps', slug: 'cloud-vps-core-4' }, '200 GB SSD'), true);
  assert.equal(taxonomy.storagePolicyAllows({ type: 'vps', slug: 'cloud-vps-core-4' }, '100 GB NVMe'), false);
  assert.equal(taxonomy.storagePolicyAllows({ type: 'vps', slug: 'cloud-vps-plus-4' }, '150 GB NVMe'), true);
  assert.equal(taxonomy.storagePolicyAllows({ type: 'vps', slug: 'cloud-vps-plus-4' }, '300 GB SSD'), false);
  assert.equal(taxonomy.storagePolicyAllows({ type: 'vds', slug: 'vds-s' }, '180 GB NVMe'), true);
  assert.equal(taxonomy.storagePolicyAllows({ type: 'storage-vps', slug: 'storage-vps-10' }, '300 GB SSD'), true);
});

test('canonical URLs require exact hydrated slugs and normalize current regions', () => {
  assert.equal(taxonomy.requiresExactProductSlug('cloud-vps-core-4'), true);
  assert.equal(taxonomy.requiresExactProductSlug('cloud-vps-plus-4'), true);
  assert.equal(taxonomy.titleFromSlug('cloud-vps-core-4'), 'Cloud VPS 4');
  assert.equal(taxonomy.titleFromSlug('cloud-vps-plus-4'), 'Cloud VPS Plus 4');
  assert.deepEqual(taxonomy.classifyRegion('Location: Australia [Cloud VPS Plus 8]'), {
    region_group: 'Australia', country: 'Australia', country_code: 'AU',
  });
  assert.deepEqual(taxonomy.classifyRegion('Vereinigte Staaten (Central)'), {
    region_group: 'America', country: 'United States (Central)', country_code: 'US', subregion: 'Central',
  });
});
