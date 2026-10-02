# Round 2, step 1: catalogue structure from the round-1 blob (zero cost)

Source: `spike0-raw/treg_anyapi.html` (2,035,974 bytes, `treg` anyapi scrape of `/en/vps/cloud-vps-10/`, 2026-10-01),
decoded with `ContaboPricing\Scrape\SapperLiteralDecoder`, read from `preloaded[0]`.
Everything below is generated from the blob by `CatalogStructure` (`lib/Scrape/CatalogStructure.php`) plus the
`xCountry=US` / `lang=en-us` context the page was served with. Nothing here was fetched live; the live-nav
cross-check of `/en/vps/` (plan step 1, last sentence) is still open and costs one Mode-A call.

Blob inventory: 168 products, 15 categories, 244 countries, 7 top-level nav items.
Reconciliation: 27 family plans + 141 legacy/excluded = 168 (114 of the 141 have no category at all).

## A. Families and their plans

Ground rule used throughout: **category membership = the category's own `products` map**, not `product.categoryId`.
The two disagree for Performance VPS: all six `cloud-vps-plus-*` products appear in `performance-vps.products`, but only
`cloud-vps-plus-18` carries `categoryId = performance-vps`; the other five carry `categoryId = vps`. Counting by
`categoryId` (as the plan's first pass did) yields "Performance VPS: 1 product". Membership gives 6.

#### Core VPS (`vps`, categoryId `10DEvjcxkYDo1JI3howydY`, nav `/vps/`)

Category lowestPrice: {"EUR":5.5,"USD":6.6,"GBP":5.4}

| Title | Slug | EUR | USD | GBP | Previous EUR | Availability |
|---|---|---|---|---|---|---|
| Cloud VPS 4 | `cloud-vps-core-4` | 5.5 | 6.6 | 5.4 | - | available |
| Cloud VPS 6 | `cloud-vps-core-6` | 7.5 | 9 | 7.35 | - | available |
| Cloud VPS 8 | `cloud-vps-core-8` | 14 | 16.8 | 13.7 | - | available |
| Cloud VPS 12 | `cloud-vps-core-12` | 25 | 30 | 24.4 | - | available |
| Cloud VPS 16 | `cloud-vps-core-16` | 37 | 44.5 | 36 | - | available |
| Cloud VPS 18 | `cloud-vps-core-18` | 49 | 58.8 | 47.7 | - | available |

#### Performance VPS (`performance-vps`, categoryId `7lZk81jQ5rYYlc436aSOZy`, nav `/vps-performance/`)

Category lowestPrice: {"EUR":13.5,"USD":16.25,"GBP":13}

| Title | Slug | EUR | USD | GBP | Previous EUR | Availability |
|---|---|---|---|---|---|---|
| Cloud VPS Plus 4 | `cloud-vps-plus-4` | 13.5 | 16.25 | 13 | - | available |
| Cloud VPS Plus 6 | `cloud-vps-plus-6` | 19 | 23 | 18.25 | - | available |
| Cloud VPS Plus 8 | `cloud-vps-plus-8` | 35 | 42 | 33.75 | - | available |
| Cloud VPS Plus 12 | `cloud-vps-plus-12` | 59 | 71 | 56.75 | - | available |
| Cloud VPS Plus 16 | `cloud-vps-plus-16` | 79 | 95 | 76 | - | available |
| Cloud VPS Plus 18 | `cloud-vps-plus-18` | 99 | 119 | 95 | - | available |

#### Max Performance VPS (`vds`, categoryId `3uwQtVY6ygQ7FN3hfT5Rb1`, nav `/vps-dedicated/`)

Category lowestPrice: {"EUR":49,"USD":59,"GBP":48}

| Title | Slug | EUR | USD | GBP | Previous EUR | Availability |
|---|---|---|---|---|---|---|
| Cloud VDS S | `vds-s` | 49 | 59 | 48 | - | available |
| Cloud VDS M | `vds-m` | 62 | 74.5 | 61 | - | available |
| Cloud VDS L | `vds-l` | 87 | 104.5 | 85.5 | - | available |
| Cloud VDS XL | `vds-xl` | 118 | 142 | 116 | - | available |
| Cloud VDS XXL | `vds-xxl` | 174 | 209 | 171 | - | available |

#### Storage VPS (`storage-vps`, categoryId `4UpVqEYIJEouRdSmd7fXb9`, nav `/storage-vps/`)

Category lowestPrice: {"EUR":5.5,"USD":6.6,"GBP":5.4}

| Title | Slug | EUR | USD | GBP | Previous EUR | Availability |
|---|---|---|---|---|---|---|
| Storage VPS 10 | `storage-vps-10` | 5.5 | 6.6 | 5.4 | - | available |
| Storage VPS 20 | `storage-vps-20` | 7.5 | 9 | 7.35 | - | available |
| Storage VPS 30 | `storage-vps-30` | 14 | 16.8 | 13.7 | - | available |
| Storage VPS 40 | `storage-vps-40` | 25 | 30 | 24.4 | - | available |
| Storage VPS 50 | `storage-vps-50` | 37 | 44.5 | 36 | - | available |

#### Dedicated Servers (`dedicated-servers`, categoryId `59t0KpSNCisB5B2rYidaKU`, nav `/dedicated-servers/`)

Category lowestPrice: {"EUR":129,"USD":155,"GBP":126}

| Title | Slug | EUR | USD | GBP | Previous EUR | Availability |
|---|---|---|---|---|---|---|
| AMD Ryzen 12 Cores | `amd-ryzen-12-cores` | 129 | 155 | 126 | 149 | available |
| AMD Genoa 24 Cores | `amd-genoa-24-cores` | 199 | 239 | 195 | - | available |
| AMD Turin 32 Cores | `ds-40` | 399 | 479 | 391 | 285 | available |
| AMD Turin 64 Cores | `ds-50` | 699 | 839 | 685 | - | available |

#### GPU VPS (`gpu-vps`, categoryId `XoA1rec97V3geJrEccFLf`, nav `/gpu-vps/`)

Category lowestPrice: {"EUR":999,"USD":1199,"GBP":969}

| Title | Slug | EUR | USD | GBP | Previous EUR | Availability |
|---|---|---|---|---|---|---|
| GPU VPS | `gpu-vps-plus-18` | 999 | 1199 | 969 | - | available |

Notes on availability: `outOfStock` is not a boolean in this blob. Values seen on products: `null`, `"coming-soon"`,
`"bright"`; `unavailable` is `null`/`false`/`true`. All 27 family plans are `null`/`null` = available.
The scorecard maps `unavailable===true` to `unavailable`, `outOfStock===true` to `out_of_stock`, a non-empty string to
that string verbatim (e.g. `coming-soon`), else `available`.

### A2. Raw category table (all 15)

| categoryId | Title | Slug | Members (category.products) | Products with product.categoryId = this | lowestPrice EUR | Role |
|---|---|---|---|---|---|---|
| `37KuQeWlXpe0WX0wig0Mj5` | VPS MP | `vps-mp` | 6 | 6 | 7.5 | legacy / excluded |
| `1gxfCS9nCz9bWGpRcaUm3F` | VPS HP | `vps-hp` | 6 | 6 | 11 | legacy / excluded |
| `4L3VylxZmiztdZqFkP34l3` | Object Storage | `object-storage` | 3 | 2 | 2.49 | legacy / excluded |
| `10DEvjcxkYDo1JI3howydY` | VPS | `vps` | 11 | 11 | 5.5 | FAMILY Core VPS (minus `-plus-` slugs) |
| `3uwQtVY6ygQ7FN3hfT5Rb1` | Max Performance VPS | `vds` | 5 | 5 | 49 | FAMILY Max Performance VPS |
| `2nq5fL0OnuNDzjB5HArthJ` | Limited SP Sale | `limited-sp-sale` | 0 | 0 | null | legacy / excluded |
| `XoA1rec97V3geJrEccFLf` | GPU VPS | `gpu-vps` | 1 | 1 | 999 | FAMILY GPU VPS |
| `7lZk81jQ5rYYlc436aSOZy` | Performance VPS | `performance-vps` | 6 | 1 | 13.5 | FAMILY Performance VPS |
| `6mcrtMtN4U2NnjaUW45JKn` | Virtual Private Servers (2026) | `vps-2026` | 6 | 6 | 4.5 | legacy / excluded |
| `59t0KpSNCisB5B2rYidaKU` | Dedicated Servers | `dedicated-servers` | 4 | 4 | 129 | FAMILY Dedicated Servers |
| `4UpVqEYIJEouRdSmd7fXb9` | Storage VPS | `storage-vps` | 5 | 5 | 5.5 | FAMILY Storage VPS |
| `7Ikb8gqyfVCUrz3jZZqHQM` | Limited SC Sale | `limited-sc-sale` | 2 | 2 | 8 | legacy / excluded |
| `tV7FhsG1LvIuWgNsXNS17` | Object Storage Category | `object-storage-category` | 1 | 1 | 2.49 | legacy / excluded |
| `5T4zUfAFtQXErsCUHrw8P5` | Outlet Server | `outlet-server` | 4 | 4 | null | legacy / excluded |
| `mnxcqg2lPeA995Nbj1OP3` | Private Sale | `private-sale` | 0 | 0 | null | legacy / excluded |


### B. Legacy / excluded products by pattern (141 records)

| Group | Count | Examples |
|---|---|---|
| C-nvme (core-count, coming-soon) | 19 | storage-vps-16c €61.5; cloud-vps-14c-nvme €33.5; cloud-vps-14c €33.5; cloud-vps-10c-nvme €26; storage-vps-10c €33.5 |
| category:limited-sc-sale | 2 | cloud-vps-30-sc €8; cloud-vps-40-sc €15 |
| category:object-storage | 3 | european-union €2.49; singapore €2.99; united-states €2.49 |
| category:outlet-server | 4 | outlet-server-1219 ; outlet-server-1011 ; outlet-server-1715 ; outlet-server-1357  |
| category:vps-2026 | 6 | cloud-vps-72 €25; cloud-vps-24 €7; cloud-vps-48 €14; cloud-vps-12 €4.5; cloud-vps-120 €49 |
| category:vps-hp | 6 | cloud-vps-core-4-hp €11; cloud-vps-core-12-hp €36; cloud-vps-core-18-hp €62; cloud-vps-core-16-hp €50; cloud-vps-core-8-hp €22 |
| category:vps-mp | 6 | cloud-vps-core-18-mp €52; cloud-vps-core-16-mp €40; cloud-vps-core-12-mp €27; cloud-vps-core-8-mp €16; cloud-vps-core-6-mp €10 |
| core-nvme (2026) | 6 | cloud-vps-core-8-nvme €14; cloud-vps-18-nvme-2026 €49; cloud-vps-core-16-nvme €37; cloud-vps-core-12-nvme €25; cloud-vps-core-6-nvme €7.5 |
| dedicated-legacy (ds-*, amd-*, intel-*) | 12 | [AMD 9 7900 Ryzen 12 Cores Dedicated Server] €149; ds-6 €119; ds-5 €179; ds-4 €199; ds-3 €121 |
| dummy placeholder (*-dummy) | 5 | cloud-vps-20-dummy €7; cloud-vps-10-dummy €4.5; amd-turin-64-dummy €666; amd-turin-32-dummy €299; cloud-vps-4c-dummy €4.05 |
| legacy cloud-vps-N | 15 | cloud-vps-core-2 €3.9; cloud-vps-20 €7.5; cloud-vps-10 €5.5; cloud-vps-60 €49; cloud-vps-50 €37 |
| nvme legacy (-nvme) | 20 | [Cloud VPS 20 NVMe] €7.5; [Cloud VPS 10 NVMe] €5.5; [Cloud VPS 60 NVMe] €49; [Cloud VPS 50 NVMe] €37; [Cloud VPS 40 NVMe] €25 |
| roman-numeral generation (I-VI SSD/NVMe/AS) | 22 | [Cloud VPS II SSD] €7; [Cloud VPS VI SSD] €49; [Cloud VPS VI AS NVMe] €49; [Cloud VPS VI NVMe] €49; [Cloud VPS VI AS SSD] €49 |
| sale-variant (SP/SC) | 8 | cloud-vps-10-sp €5.5; [Cloud VPS 10 SP NVMe] €5.5; [Cloud VPS 40 SC NVMe] €15; cloud-vps-30-sp €16; [Cloud VPS 30 SP NVMe] €16 |
| storage-vps legacy | 6 | storage-vps-1 €4.5; storage-vps-2 €9.5; storage-vps-3 €14; storage-vps-4 €26; storage-vps-5 €33.5 |
| vds legacy | 1 | vds-xs €14.99 |

Uncategorised (no category membership): 114


One-line hypothesis per legacy group:

| Group | Hypothesis |
|---|---|
| `category:vps-hp` (6), `category:vps-mp` (6) | Same specs and same 6 sizes as Core VPS (4/6/8/12/16/18 vCPU), identical category subtitle, higher price (HP about 2x Core at size 4: 11 vs 5.5; MP 7.5). Not linked from the nav. Price-tier mirrors of Core VPS (HP = high price, MP = mid price), probably campaign/regional/A-B pricing. Unverified. Do not sell as separate families. |
| `category:vps-2026` (6) | Previous generation re-launch: "Cloud VPS 12/24/48/72/96/120" with 6-24 vCPU, 100-500 GB NVMe, EUR 4.5-49. Superseded by the Core lineup (`cloud-vps-core-*`). Not in nav. |
| `category:limited-sc-sale` (2) | "Cloud VPS 30/40 SC" limited sale, no nav link, no traffic spec. Campaign leftovers. |
| `category:object-storage` (3) | The three object-storage region products (`european-union` EUR 2.49, `singapore` EUR 2.99, `united-states` EUR 2.49) = unit price per 250 GB step. These are NOT plans; they price the "N GB Object Storage in {product_location}" configurator addon (see C). |
| `category:outlet-server` (4) | "Server Deals" nav entry (`/server-outlet/`); no price in the blob (priced per stock item). |
| `core-nvme (2026)` (6) | `cloud-vps-core-N-nvme` / "(2026)" interim naming of the Core lineup before the SSD/NVMe merge ("400GB SSD / 200GB NVMe" specs). Superseded. |
| `legacy cloud-vps-N` (15) | The pre-2026 `cloud-vps-10 ... -60`, `-1 ... -6` slugs (the 16-slug `PlanUrlList` defaults came from here). |
| `nvme legacy (-nvme)` (20), `roman-numeral generation` (22) | Pre-slug generation (no slug at all, titles only: "Cloud VPS II SSD", "AS NVMe"). Dead records kept in the CMS. |
| `C-nvme (core-count, coming-soon)` (19) | "VPS 14 Cores NVMe", `cloud-vps-10c-nvme`, `storage-vps-10c`: all `outOfStock="coming-soon"`; a never-launched core-count naming scheme. |
| `sale-variant (SP/SC)` (8) | "Cloud VPS 10/20/30 SP", "30/40 SC" sale SKUs, no category. |
| `dedicated-legacy` (12), `dummy placeholder` (5) | Older dedicated lineups (`ds-1..6`, `amd-*`, `intel-*`) and CMS test records (`*-dummy`, "Cloud VPS 4C (Fake)", "DS 50" at EUR 666). The live Dedicated family is the 4 products in `dedicated-servers`. |
| `storage-vps legacy` (6), `vds legacy` (1) | `storage-vps-1..6`, `-2c/-4c...` and `vds-xs` (`outOfStock="bright"`): older sizes not in the categories. |

## C. Addon groups

`addons` is a per-product map `id -> {id, title, groupId?, price{EUR,GBP,USD}?, setupPrice{...}?, osId?, outOfStock?}`.
The Core VPS 4 product carries 97 addons (Core 12 carries 100). Important structural facts:

1. **Price addons carry a `groupId`; marker/default addons do not.** Group-less addons are OS images (`osId`), region
   default ("European Union"), included-size markers ("100 GB SSD"), object-storage sizes, and UI text. They have no
   `price`.
2. **Region, storage-upgrade and backup groupIds are per product, not per catalogue.** Cloud VPS 4 uses 2214 (backup),
   2215 (200 GB SSD), 2219 (India), 2220-2225 (other regions); other plans use their own group ids (e.g. 2227/2239/2251/2275
   for the larger SSD tiers, 2227..2365 for NVMe tiers, 1733-1737 and 1952-1958 India for other products). The
   same region therefore has a **different price on each plan** (India: 2.4 on Core 4, 20.6-73 on other products), so
   regions must be extracted per plan, never once per catalogue.
3. **Object storage has no price on the addon.** "250 GB / 500 GB / 750 GB / 1 TB Object Storage in {product_location}" are
   group-less; the price is `object-storage-region-product.price.EUR x (size / 250 GB)`. Check against the screenshot:
   Singapore EUR 2.99 x 4 = 11.96 net, x 1.18 = 14.11 gross = "1 TB Object Storage in Asia EUR 14.11". Confirmed.
4. Titles can be localised or oddly shaped: `Vereinigte Staaten (Central)` (group 2319), `Location: Australia [Cloud VPS Plus 8]`
   (group 2314). Classify by pattern, not by an exact title list.

Groups attached to the 27 family plans, collapsed (full detail is regenerated by `CatalogStructure::addonGroups()`;
the whole blob has 847 distinct group ids once per-product duplicates are counted):

| Class | groupId(s) | Titles | Max products carrying it | EUR range (monthly) | Max setup EUR |
|---|---|---|---|---|---|
| app | 951 | LAMP | 27 | 0..0 | 0 |
| app | 1168 | Webmin + LAMP | 22 | 0..0 | 0 |
| app | 1470 | Docker | 27 | 0..0 | 0 |
| app | 1585 | Bitcoin Node | 8 | 0..0 | 0 |
| app | 1586 | Ethereum Staking Node | 8 | 0..0 | 0 |
| app | 1587 | IPFS Node | 23 | 0..0 | 0 |
| app | 1588 | Flux Node | 23 | 0..0 | 0 |
| app | 1589 | Horizen Node | 23 | 0..0 | 0 |
| app | 2052 | WireGuard Server | 23 | 0..0 | 0 |
| app | 2053 | n8n Server | 23 | 0..0 | 0 |
| app | 2054 | Nextcloud Server | 23 | 0..0 | 0 |
| app | 2055 | Gitlab Server | 23 | 0..0 | 0 |
| app | 2134 | OpenClaw Server | 23 | 0..0 | 0 |
| app | 2213 | Coolify Server | 23 | 0..0 | 0 |
| app | 2358 | Zeroclaw Server | 23 | 0..0 | 0 |
| app | 2359 | Dokploy Server | 23 | 0..0 | 0 |
| app | 2360 | Hermes Agent Server | 23 | 0..0 | 0 |
| app | 2361 | Paperclip Server | 22 | 0..0 | 0 |
| app | 2362 | Ollama Server | 15 | 0..0 | 0 |
| app | none:app | DevOps Features | 23 | - | 0 |
| backup | 2214..2334 (11 groups) | Auto Backup | 1 | 1.65..12.5 | 0 |
| ftp_storage | 1019 | 2 TB FTP Storage | 27 | 36.79..36.79 | 0 |
| ftp_storage | 1020 | 500 GB FTP Storage | 27 | 13.79..13.79 | 0 |
| ftp_storage | 1021 | 100 GB FTP Storage | 27 | 4.59..4.59 | 0 |
| ftp_storage | 1022 | 1 TB FTP Storage | 27 | 22.99..22.99 | 0 |
| monitoring | 211 | Full Monitoring | 27 | 11.49..11.49 | 0 |
| object_storage | none:object_storage | 250 GB Object Storage in {product_location}; 500 GB Object Storage in {product_location}; 750 GB Object Storage in {product_location}; 1 TB Object Storage in {product_location} | 23 | - | 0 |
| os_image | 1219, 1924, 1943 | Windows Server 2022 Standard; Windows Server 2019 Standard; Windows Server 2016 Standard | 1 | 50..199 | 199 |
| os_image | 1338 | Windows Server 2016 Standard; Windows Server 2025 Standard; Windows Server 2019 Standard; Windows Server 2022 Standard; Windows Server 2012R2 Standard | 5 | 50..50 | 50 |
| os_image | 1471 | Add Custom Images Storage (25 GB) | 23 | 1.19..1.19 | 0 |
| os_image | 1715 | Windows Server 2019 Standard; Windows Server 2016 Standard; Windows Server 2022 Standard | 3 | 72..72 | 72 |
| os_image | 2022, 2029, 2030, 2039, 2042 | Windows Server 2016 Datacenter | 1 | 4.4..50 | 0 |
| os_image | 2032, 2037 | Windows Server 2022 Datacenter; Windows Server 2019 Datacenter; Windows Server 2025 Datacenter | 1 | 8..53 | 0 |
| os_image | 2035 | Windows Server 2019 Datacenter; Windows Server 2022 Datacenter; Windows Server 2025 Datacenter | 1 | 28..28 | 0 |
| os_image | 2036, 2040 | Windows Server 2025 Datacenter; Windows Server 2019 Datacenter; Windows Server 2022 Datacenter | 1 | 5.2..17 | 0 |
| os_image | 2216..2348 (12 groups) | Windows Server Datacenter 2016 | 2 | 8..115 | 0 |
| os_image | 2217, 2301 | Windows Server Datacenter 2019; Windows Server Datacenter 2025; Windows Server Datacenter 2022 | 1 | 8..25.5 | 0 |
| os_image | 2229, 2241, 2265, 2337 | Windows Server Datacenter 2025; Windows Server Datacenter 2022; Windows Server Datacenter 2019 | 1 | 17..95 | 0 |
| os_image | 2253, 2277, 2289, 2313, 2325, 2349 | Windows Server Datacenter 2022; Windows Server Datacenter 2019; Windows Server Datacenter 2025 | 2 | 12..115 | 0 |
| os_image | none:os_image | Arch Linux; Ubuntu 26.04; Ubuntu 19.10 (64 Bit); AlmaLinux 10; Debian 12; ... (15 titles) | 27 | - | 0 |
| other | 518 | Fast-track Setup | 4 | 0..0 | 34.49 |
| other | 666 | Software RAID | 3 | 0..0 | 0 |
| other | 903 | SSL certificate | 27 | 0..0 | 74.99 |
| other | 1002 | Managed | 22 | 109.99..109.99 | 0 |
| other | 1073 | Hardware RAID | 1 | - | 0 |
| other | 1143 | SSL certificate (wildcard) | 27 | 0..0 | 224.99 |
| other | 1226 | pfSense Hardware Firewall | 4 | 55.99..55.99 | 109.99 |
| other | 1238 | 10 Gbit/s port | 2 | 109.99..109.99 | 109.99 |
| other | 1263 | Additional IP adress | 27 | 3.5..3.5 | 0 |
| other | 1353 | GeForce GT 1030 2GB | 3 | 19.59..19.59 | 16.49 |
| other | 1384 | 80 TB Out + Unlimited In; Unlimited and Unmetered Traffic | 9 | 86.29..86.29 | 0 |
| other | 1385 | 160 TB Out + Unlimited In; Unlimited and Unmetered Traffic | 6 | 172.49..172.49 | 0 |
| other | 1386 | 243 TB Out + Unlimited In | 6 | 258.79..258.79 | 0 |
| other | 1387 | 320 TB Out + Unlimited In; Unlimited and Unmetered Traffic | 6 | 344.99..344.99 | 0 |
| other | 1477, 1489 | Private Networking Enabled | 19 | 2.29..5.79 | 0 |
| other | 1478 | Nvidia Tesla A2 16 GB | 4 | 219.99..219.99 | 57.49 |
| other | 1501 | Firewall Enabled | 6 | 0.99..0.99 | 0 |
| other | 1567 | 128 GB DDR ODECC RAM | 1 | 90..90 | 0 |
| other | 1918 | 256 GB DDR5 ECC RAM | 2 | 200..200 | 0 |
| other | 1919 | 512 GB DDR5 ECC RAM | 2 | 600..600 | 0 |
| other | 1920, 1923 | 768 GB DDR5 ECC RAM | 2 | 900..1000 | 0 |
| other | 1921 | 384 GB DDR5 ECC RAM | 1 | 300..300 | 0 |
| other | 1922 | 576 GB DDR5 ECC RAM | 1 | 600..600 | 0 |
| other | 1933 | 1152 GB DDR5 ECC RAM | 1 | 1500..1500 | 0 |
| other | 1935 | 960 GB DDR5 ECC RAM | 1 | 1200..1200 | 0 |
| other | 2046 | Free IPMI (iLO) Access | 4 | 0..0 | 0 |
| other | 2375 | GPU VPS | 1 | 0..0 | 0 |
| other | none:other | None; Unmanaged; Password; No Private Networking; In order to use SSH Keys you can add them in the Customer Control Panel later. Your password will not be sent via email. Be sure to remember it for Windows access. If you forget the password, you will need to reinstall your server.; ... (17 titles) | 27 | - | 0 |
| panel | 639 | Webmin | 27 | 0..0 | 0 |
| panel | 1040, 1044 | Plesk Obsidian Web Host Edition | 23 | 36.5..41.5 | 0 |
| panel | 1041, 1045 | Plesk Obsidian Web Pro Edition | 23 | 19..19 | 0 |
| panel | 1043, 1047 | Plesk Obsidian Web Admin Edition | 23 | 12..12 | 0 |
| panel | 1273..1294 (22 groups) | cPanel/WHM (5 accounts); cPanel/WHM (30 accounts); cPanel/WHM (50 accounts); cPanel/WHM (100 accounts); cPanel/WHM (150 accounts); ... (22 titles) | 27 | 21.75..398.5 | 0 |
| region | 1328..2363 (25 groups) | United States (Central) | 1 | 1.05..32 | 0 |
| region | 1395..2353 (23 groups) | Asia (Singapore) | 1 | 2.5..78 | 0 |
| region | 1407..2356 (23 groups) | United States (East) | 1 | 1.55..47 | 0 |
| region | 1418..2357 (23 groups) | United States (West) | 1 | 1.3..39 | 0 |
| region | 1494..2354 (23 groups) | United Kingdom | 1 | 1.05..32 | 0 |
| region | 1505..2350 (22 groups) | Australia (Sydney) | 1 | 2.15..65 | 0 |
| region | 1541..2352 (23 groups) | Asia (Japan) | 1 | 2.55..79 | 0 |
| region | 1733..2351 (23 groups) | Asia (India) | 1 | 2.4..73 | 0 |
| region | 2314 | Location: Australia [Cloud VPS Plus 8] | 1 | 16.55..16.55 | 0 |
| region | 2319 | Vereinigte Staaten (Central) | 1 | 8.1..8.1 | 0 |
| region | none:region | European Union | 27 | - | 0 |
| storage_upgrade | 1334, 1573 | 4 TB SSD | 5 | 32.9..103.49 | 0 |
| storage_upgrade | 1335, 1571, 2263 | 1 TB SSD | 5 | 7.5..24.99 | 0 |
| storage_upgrade | 1336, 1572 | 2 TB SSD | 5 | 17.9..39.09 | 0 |
| storage_upgrade | 1568, 9999 | 1 TB NVMe; 1 TB SSD | 4 | 0..10 | 0 |
| storage_upgrade | 1569 | 2 TB NVMe | 3 | 19..19 | 0 |
| storage_upgrade | 1574 | 16 TB HDD | 1 | 21.9..21.9 | 0 |
| storage_upgrade | 1917 | 4 TB NVMe | 2 | 49..49 | 0 |
| storage_upgrade | 2215 | 200 GB SSD | 1 | 1.5..1.5 | 0 |
| storage_upgrade | 2227 | 400 GB SSD | 1 | 3..3 | 0 |
| storage_upgrade | 2239 | 600 GB SSD | 1 | 4.5..4.5 | 0 |
| storage_upgrade | 2251 | 800 GB SSD | 1 | 6..6 | 0 |
| storage_upgrade | 2275 | 1.2 TB SSD | 1 | 9..9 | 0 |
| storage_upgrade | 2287 | 300 GB NVMe | 1 | 3.75..3.75 | 0 |
| storage_upgrade | 2299 | 600 GB NVMe | 1 | 7.5..7.5 | 0 |
| storage_upgrade | 2311 | 900 GB NVMe | 1 | 11.25..11.25 | 0 |
| storage_upgrade | 2323 | 1.2 TB NVMe | 1 | 15..15 | 0 |
| storage_upgrade | 2335 | 1.5 TB NVMe | 1 | 18.75..18.75 | 0 |
| storage_upgrade | 2347, 2365 | 1.8 TB NVMe | 1 | 22.5..22.5 | 0 |
| storage_upgrade | none:storage_upgrade | 300 GB NVMe; 600 GB SSD; 250 GB NVMe; 500 GB SSD; 200 GB NVMe; ... (21 titles) | 18 | 0..0 | 0 |

Classification summary (class, what it is, where it appears on the checkout):

| Class | Groups | Notes |
|---|---|---|
| region surcharge | per-product groups, titles `Asia (India\|Japan\|Singapore)`, `Australia (Sydney)`, `United Kingdom`, `United States (East\|Central\|West)`; `European Union` = included default | Asia (Singapore) carries `outOfStock: true` on Cloud VPS 4. |
| storage upgrade | 2215 (200 GB SSD, Core 4) and the per-plan SSD/NVMe tiers; 1006/1007/1334-1336/1568-1574/1917 for dedicated disks | Group-less "100 GB SSD" / "50 GB NVMe" are included-size markers, price null. |
| backup | 2214 (Cloud VPS 4), 11 per-plan groups up to 2334 | Titled "Auto Backup"; Core 4 = EUR 1.65 (screenshot 1.95 gross). GPU VPS has none. |
| object storage | group-less, price derived from the 3 object-storage products | 250/500/750/1000 GB. |
| control panel | 1273-1294 cPanel/WHM tiers (5..1000 accounts, EUR 21.75..398.5), 1040-1047 Plesk (Web Admin 12 / Pro 19 / Host 36.5-41.5), 639 Webmin | |
| OS image | group-less, `osId` set (Ubuntu 22.04/24.04/26.04, Debian 12/13, AlmaLinux 9/10, Rocky 8/9/10, Arch, FreeBSD, Proxmox, custom); priced Windows Server groups 2216-2349 (EUR 8..115) | "Ubuntu 24.04" has no price = included, matching "Ubuntu 24.04 Included". |
| app image | 2052-2055, 2134, 2213, 2358-2362 (WireGuard, n8n, Nextcloud, GitLab, OpenClaw, Coolify, Zeroclaw, Dokploy, Hermes, Paperclip, Ollama), 951 LAMP, 1168 Webmin+LAMP, 1470 Docker, 1585-1589 node images | all EUR 0 |
| monitoring | 211 Full Monitoring EUR 11.49 | |
| FTP storage | 1019-1022 (100 GB 4.59, 500 GB 13.79, 1 TB 22.99, 2 TB 36.79) | |
| other | 903/1143 SSL (setup 74.99/224.99), 1002 Managed 109.99, 1263 extra IP 3.5, 1477/1489 private networking, 1501 firewall, 1384-1387 traffic tiers (dedicated), RAM/GPU/RAID hardware (dedicated), 518 fast-track setup | Dedicated-only hardware options carry `setupPrice` (e.g. 10 Gbit/s port 109.99 setup 109.99). |

## C2. Screenshot ground truth vs the blob

| Screenshot value (gross, 18 % VAT) | Blob value (EUR net) | net x 1.18 | Match |
|---|---|---|---|
| Cloud VPS 4 EUR 6.49 | `cloud-vps-core-4.price.EUR` 5.5 | 6.49 | yes |
| Asia (India) EUR 2.83 | addon group 2219 `Asia (India)` 2.4 | 2.832 | yes |
| 200 GB SSD EUR 1.77 | group 2215 1.5 | 1.77 | yes |
| Auto Backup EUR 1.95 | group 2214 1.65 | 1.947 | yes |
| 1 TB Object Storage in Asia EUR 14.11 | 4 x Singapore 2.99 = 11.96 | 14.113 | yes (derived) |
| Ubuntu 24.04 Included | `osId 332`, no price | n/a | yes |
| Cloud VPS 4 12 m -15 %, 24 m -20 % | period discount EUR 9.9 / 26.4 on 5.5/month | 9.9/66 = 15 %, 26.4/132 = 20 % | yes |
| GPU VPS EUR 999 net | `gpu-vps-plus-18.price.EUR` 999 (USD 1199, GBP 969) | | yes |
| GPU specs: RTX 6000, 18 vCPU, 96 GB, 900 GB NVMe, 5 snapshots, 1 Gbit/s | spec titles `GPU - NVIDIA RTX 6000`, `18 vCPU Cores`, `96 GB RAM`, `900 GB NVMe`, `5 Snapshots`, `1 Gbit/s Port` | | yes |
| GPU 12 m EUR 1002 / 24 m EUR 943.06 (gross, per month) | discount EUR 1798.2 (= 15 % of 11 988) / 4795.2 (= 20 % of 23 976) | (999 - 1798.2/12) x 1.18 = 1001.997; (999 - 4795.2/24) x 1.18 = 943.056 | yes |

Period semantics: `periods[].discount.EUR` is the **total discount over the whole period**, not per month.
`effective_monthly = (price x months - discount) / months`; `total = price x months - discount`.
Dedicated Servers offer only 1/3/6/12 months (`ds-50`: 104.85 = 5 % of 2097 at 3 m; 838.8 = 10 % of 8388 at 12 m).
The GPU "GPU VPS" product title is just `GPU VPS`; its slug is `gpu-vps-plus-18` (not `gpu-vps`).

## D. Countries, nav and landingPageNav

- `countries`: map of 244 ISO codes to `{name, cc, currency, vatRate, vat_rate_private, vat_rate_company, vat_verification, states[]}`.
  Currency split: EUR 49, USD 194, GBP 1 (GB). Only EUR/USD/GBP exist, which is why the scorecard prices are EUR/USD/GBP.
  India: currency **USD**, `vatRate 0.18`, 36 states. Germany EUR 19 %, GB 20 %, US 0 %.
  The page was served with `xCountry=US`, `lang=en-us`; prices in the blob are for all three currencies at once, so
  geo does not change the blob (not tested for other `xCountry`).
- `navItems` (order, titles, links):
  1. **VPS**: Core VPS `/vps/`, Performance VPS `/vps-performance/` ("High-Performance AI-Ready Virtual Private Servers"),
     Max Performance VPS `/vps-dedicated/` ("Virtual Dedicated Servers with full separation"), Storage VPS `/storage-vps/`,
     Windows VPS `/windows-servers-vps/` (no category in the blob), then non-link headings APPS & PANELS, FEATURES.
  2. **Dedicated Servers**: Dedicated Servers `/dedicated-servers/`, Server Deals `/server-outlet/`, Max Performance VPS `/vps-dedicated/` (repeated).
  3. **GPU VPS**: GPU VPS `/gpu-vps/` ("Dedicated NVIDIA RTX PRO 6000"; note the nav says RTX PRO 6000, the product spec says RTX 6000), plus use-case pages
     and Managed GPU `/gpu-cloud/` (a different, externally managed offer).
  4. Apps & Panels, 5. More (Domains, GPU Servers, CDN), 6. Pricing, 7. Company.
- `landingPageNav`: a single entry `{title: Support, link: https://help.contabo.com/support/home}`. It carries **no** family information.

**Nav link != category slug** for two families: `/vps-performance/` is category `performance-vps` and `/vps-dedicated/` is
category `vds`. Landing-page URLs for fetching must therefore come from the nav links (`/en` + link), not from category slugs.

## E. Ambiguity: VPS / VPS HP / VPS MP / Performance VPS / Max Performance VPS

| Blob category | Public nav name | Evidence | Decision |
|---|---|---|---|
| `vps` ("VPS") | **Core VPS** (`/vps/`) | `cloud-vps-core-N`; but its `products` map ALSO lists the five `cloud-vps-plus-*` (11 total). | Family, minus slugs matching `-plus-\d+$`. |
| `performance-vps` ("Performance VPS") | **Performance VPS** (`/vps-performance/`) | Same title in category and nav; 6 `cloud-vps-plus-*` members, but only Plus 18 has this `categoryId`. | Family; membership via `products` map. |
| `vds` ("Max Performance VPS") | **Max Performance VPS** (`/vps-dedicated/`) | Category title equals nav title; slug `vds`, products `vds-s..xxl` titled "Cloud VDS". | Family; key is the slug `vds`, display name from nav. |
| `vps-hp` ("VPS HP") | none | No nav link; no `/vps-hp/` page link; same specs as Core at higher price. | Excluded (legacy). NOT "Performance VPS": Performance VPS is the `cloud-vps-plus-*` NVMe line with 5 snapshots, different specs. |
| `vps-mp` ("VPS MP") | none | Same as HP at a mid price. | Excluded (legacy). NOT "Max Performance VPS" (that is `vds`). |

So the plan's open decision 1 ("which site family are VPS HP / VPS MP") has an answer from the blob alone: neither. The
live `/en/vps/` fetch should still confirm that HP/MP do not render (still open).

## F. Proposed stable mapping rule (implemented in `CatalogStructure`)

1. **Family = category**, keyed by the category **slug** (stable), displayed with the **nav title** (fallback: category title).
   Allow-list (nav order): `vps` Core VPS, `performance-vps` Performance VPS, `vds` Max Performance VPS, `storage-vps` Storage VPS,
   `dedicated-servers` Dedicated Servers, `gpu-vps` GPU VPS. Stable `categoryId`s are carried in the output (`category_id`) so a
   rename of the title or slug is detected as drift rather than silently remapped.
2. **Ordering** = position of the family's nav link in `navItems` (first occurrence); unknown nav falls back to the allow-list order.
3. **Membership** = `categories[slug].products` keys, not `product.categoryId`. In `vps` additionally exclude slugs matching `-plus-\d+$`.
4. **Everything else is `legacy[]`** (including `vps-hp`, `vps-mp`, `vps-2026`, sale/outlet/object-storage categories and the
   114 uncategorised records), excluded unless explicitly allow-listed. Group label per pattern is in section B.
5. **Drift alarms** the consumer should raise (not implemented in this step): an allow-listed slug missing from `categories`;
   a family with 0 plans; a category not in the allow-list that has a nav link; a family whose plan count changes by more than 50 %.
6. **Options per plan** come from the product's own `addons` map (never shared across plans): regions (class region), storage upgrades
   (priced storage items), backup, object storage (price derived: unit product price x size / 250 GB), OS images (`osId`),
   apps, panels. Dedicated-only hardware, traffic and SSL items are not in the scorecard.
