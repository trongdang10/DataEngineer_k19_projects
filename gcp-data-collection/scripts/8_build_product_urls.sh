#!/usr/bin/env bash
# Build product_id → URL mappings inside MongoDB using aggregations.
# Writes results to collections: product_urls and product_urls_reco.
set -euo pipefail

MONGO_URI="mongodb://localhost:27017"
DB="countly"

cat <<'JS' | mongosh "$MONGO_URI/$DB"
const events = [
  "view_product_detail",
  "select_product_option",
  "select_product_option_quality",
  "add_to_cart_action",
  "product_detail_recommendation_visible",
  "product_detail_recommendation_noticed",
];

// product_id/current_url -> product_urls
db.summary.aggregate([
  // Keep only target events and rows that have product_id/viewing_product_id plus a current_url
  { $match: { collection: { $in: events }, $or: [{ product_id: { $ne: "" } }, { viewing_product_id: { $ne: "" } }] } },
  { $project: { pid: { $ifNull: ["$product_id", "$viewing_product_id"] }, url: "$current_url" } },
  { $match: { pid: { $ne: "" }, url: { $ne: "" } } },
  { $group: { _id: "$pid", url: { $first: "$url" } } },
  { $out: "product_urls" }
]);

// product_view_all_recommend_clicked -> product_urls_reco
db.summary.aggregate([
  { $match: { collection: "product_view_all_recommend_clicked", viewing_product_id: { $ne: "" }, referrer_url: { $ne: "" } } },
  { $project: { pid: "$viewing_product_id", url: "$referrer_url" } },
  { $group: { _id: "$pid", url: { $first: "$url" } } },
  { $out: "product_urls_reco" }
]);
JS
echo "Product URLs extraction complete."
