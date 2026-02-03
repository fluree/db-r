//! Supply chain domain model for benchmark data generation.
//!
//! Generates deterministic, relational data with a seeded RNG.
//! Entity types: Category, Manufacturer, Warehouse, Distributor, Retailer,
//! Product, Order, LineItem, Shipment.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};

// --- Cardinalities per unit ---
const CATEGORIES: usize = 20;
const MANUFACTURERS: usize = 5;
const WAREHOUSES: usize = 20;
const DISTRIBUTORS: usize = 15;
const RETAILERS: usize = 50;
const PRODUCTS: usize = 200;
const ORDERS: usize = 1_000;
const LINE_ITEMS_PER_ORDER: usize = 3;
const SHIPMENTS: usize = 500;

/// Total entities per unit (for reporting).
pub const ENTITIES_PER_UNIT: usize = CATEGORIES
    + MANUFACTURERS
    + WAREHOUSES
    + DISTRIBUTORS
    + RETAILERS
    + PRODUCTS
    + ORDERS
    + ORDERS * LINE_ITEMS_PER_ORDER
    + SHIPMENTS;

// --- Name dictionaries ---

const ADJECTIVES: &[&str] = &[
    "Global", "Prime", "Swift", "Atlas", "Apex", "Summit", "Pacific", "Nordic", "Stellar", "Rapid",
    "Vertex", "Nova", "Titan", "Zenith", "Cascade", "Bolt", "Eagle", "Falcon", "Harbor", "Iron",
    "Silver", "Golden", "Blue", "Red", "Green", "Crystal", "Diamond", "Marble", "Cedar", "Pine",
    "Oak", "Elm",
];

const NOUNS: &[&str] = &[
    "Supply",
    "Logistics",
    "Trade",
    "Freight",
    "Cargo",
    "Commerce",
    "Transit",
    "Import",
    "Export",
    "Flow",
    "Chain",
    "Link",
    "Bridge",
    "Route",
    "Path",
    "Wave",
    "Grid",
    "Hub",
    "Port",
    "Dock",
    "Depot",
    "Forge",
    "Mill",
    "Works",
    "Tech",
    "Solutions",
    "Systems",
    "Group",
    "Corp",
    "Industries",
    "Partners",
    "Networks",
];

const CITIES: &[&str] = &[
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "Austin",
    "Portland",
    "Denver",
    "Seattle",
    "Boston",
    "Nashville",
    "Baltimore",
    "Memphis",
    "Louisville",
    "Milwaukee",
    "Tucson",
    "Atlanta",
    "Miami",
    "Detroit",
    "Minneapolis",
    "Charlotte",
    "Tampa",
    "Pittsburgh",
    "Cincinnati",
    "Orlando",
    "St. Louis",
];

const COUNTRIES: &[&str] = &[
    "US", "CN", "DE", "JP", "KR", "TW", "MX", "CA", "UK", "FR", "IT", "BR", "IN", "AU", "NL", "SE",
    "CH", "SG", "TH", "VN",
];

const REGIONS: &[&str] = &[
    "Northeast",
    "Southeast",
    "Midwest",
    "Southwest",
    "West Coast",
    "Pacific NW",
    "Mountain",
    "Plains",
    "Gulf Coast",
    "Mid-Atlantic",
    "New England",
    "Great Lakes",
    "Delta",
    "Piedmont",
    "Heartland",
];

const CATEGORY_NAMES: &[&str] = &[
    "Electronics",
    "Automotive",
    "Furniture",
    "Clothing",
    "Food",
    "Beverages",
    "Pharmaceuticals",
    "Chemicals",
    "Metals",
    "Plastics",
    "Paper",
    "Textiles",
    "Machinery",
    "Tools",
    "Lighting",
    "Plumbing",
    "Hardware",
    "Software",
    "Medical",
    "Agricultural",
];

const STORE_TYPES: &[&str] = &[
    "BigBox",
    "Boutique",
    "Warehouse",
    "Online",
    "Franchise",
    "Department",
    "Specialty",
    "Discount",
    "Convenience",
    "Superstore",
];

const ORDER_STATUSES: &[&str] = &["pending", "confirmed", "shipped", "delivered", "cancelled"];

const SHIPMENT_STATUSES: &[&str] = &["in_transit", "delivered", "delayed", "returned"];

fn pick<'a>(rng: &mut StdRng, list: &'a [&str]) -> &'a str {
    list[rng.gen_range(0..list.len())]
}

fn company_name(rng: &mut StdRng) -> String {
    format!("{} {}", pick(rng, ADJECTIVES), pick(rng, NOUNS))
}

/// Generate all entities for a single unit as a JSON-LD array.
///
/// The output is deterministic for any given `unit_id`.
pub fn generate_unit(unit_id: usize) -> Value {
    let mut rng = StdRng::seed_from_u64(unit_id as u64);
    let u = unit_id;

    let mut entities: Vec<Value> = Vec::with_capacity(ENTITIES_PER_UNIT);

    // Categories
    for i in 0..CATEGORIES {
        entities.push(json!({
            "@id": format!("ex:cat_{u}_{i}"),
            "@type": "ex:Category",
            "schema:name": CATEGORY_NAMES[i % CATEGORY_NAMES.len()],
            "ex:code": format!("CAT-{u:04}-{i:03}")
        }));
    }

    // Manufacturers
    for i in 0..MANUFACTURERS {
        entities.push(json!({
            "@id": format!("ex:mfr_{u}_{i}"),
            "@type": "ex:Manufacturer",
            "schema:name": company_name(&mut rng),
            "ex:country": pick(&mut rng, COUNTRIES),
            "ex:founded": rng.gen_range(1950..2020)
        }));
    }

    // Warehouses (each belongs to a manufacturer)
    for i in 0..WAREHOUSES {
        let mfr_idx = i % MANUFACTURERS;
        entities.push(json!({
            "@id": format!("ex:wh_{u}_{i}"),
            "@type": "ex:Warehouse",
            "schema:name": format!("{} Warehouse", pick(&mut rng, CITIES)),
            "ex:location": pick(&mut rng, CITIES),
            "ex:capacitySqFt": rng.gen_range(10_000..500_000),
            "ex:operator": {"@id": format!("ex:mfr_{u}_{mfr_idx}")}
        }));
    }

    // Distributors (each supplied by 2-3 warehouses)
    for i in 0..DISTRIBUTORS {
        let num_suppliers = rng.gen_range(2..=3);
        let suppliers: Vec<Value> = (0..num_suppliers)
            .map(|_| json!({"@id": format!("ex:wh_{u}_{}", rng.gen_range(0..WAREHOUSES))}))
            .collect();

        entities.push(json!({
            "@id": format!("ex:dist_{u}_{i}"),
            "@type": "ex:Distributor",
            "schema:name": format!("{} Distribution", company_name(&mut rng)),
            "ex:region": pick(&mut rng, REGIONS),
            "ex:fleetSize": rng.gen_range(10..500),
            "ex:suppliedBy": suppliers
        }));
    }

    // Retailers (each sourced from 1-2 distributors)
    for i in 0..RETAILERS {
        let num_sources = rng.gen_range(1..=2);
        let sources: Vec<Value> = (0..num_sources)
            .map(|_| json!({"@id": format!("ex:dist_{u}_{}", rng.gen_range(0..DISTRIBUTORS))}))
            .collect();

        entities.push(json!({
            "@id": format!("ex:ret_{u}_{i}"),
            "@type": "ex:Retailer",
            "schema:name": format!("{} {}", pick(&mut rng, CITIES), pick(&mut rng, STORE_TYPES)),
            "ex:city": pick(&mut rng, CITIES),
            "ex:storeType": pick(&mut rng, STORE_TYPES),
            "ex:source": sources
        }));
    }

    // Products (each made by a manufacturer, in a category)
    for i in 0..PRODUCTS {
        let mfr_idx = i % MANUFACTURERS;
        let cat_idx = i % CATEGORIES;
        entities.push(json!({
            "@id": format!("ex:prod_{u}_{i}"),
            "@type": "ex:Product",
            "schema:name": format!("{} {}-{}", pick(&mut rng, ADJECTIVES), pick(&mut rng, NOUNS), i),
            "ex:sku": format!("SKU-{u:04}-{i:05}"),
            "ex:price": (rng.gen_range(100..100_000) as f64) / 100.0,
            "ex:madeBy": {"@id": format!("ex:mfr_{u}_{mfr_idx}")},
            "ex:category": {"@id": format!("ex:cat_{u}_{cat_idx}")}
        }));
    }

    // Orders (placed by a retailer, fulfilled by a warehouse)
    for i in 0..ORDERS {
        let ret_idx = rng.gen_range(0..RETAILERS);
        let wh_idx = rng.gen_range(0..WAREHOUSES);
        let year = rng.gen_range(2020..2026);
        let month = rng.gen_range(1..=12);
        let day = rng.gen_range(1..=28);

        entities.push(json!({
            "@id": format!("ex:ord_{u}_{i}"),
            "@type": "ex:Order",
            "ex:orderDate": format!("{year:04}-{month:02}-{day:02}"),
            "ex:status": pick(&mut rng, ORDER_STATUSES),
            "ex:totalValue": (rng.gen_range(1000..500_000) as f64) / 100.0,
            "ex:placedBy": {"@id": format!("ex:ret_{u}_{ret_idx}")},
            "ex:fulfilledBy": {"@id": format!("ex:wh_{u}_{wh_idx}")}
        }));

        // Line items for this order
        for li in 0..LINE_ITEMS_PER_ORDER {
            let prod_idx = rng.gen_range(0..PRODUCTS);
            entities.push(json!({
                "@id": format!("ex:li_{u}_{i}_{li}"),
                "@type": "ex:LineItem",
                "ex:quantity": rng.gen_range(1..100),
                "ex:unitPrice": (rng.gen_range(100..10_000) as f64) / 100.0,
                "ex:order": {"@id": format!("ex:ord_{u}_{i}")},
                "ex:product": {"@id": format!("ex:prod_{u}_{prod_idx}")}
            }));
        }
    }

    // Shipments
    for i in 0..SHIPMENTS {
        let ord_idx = rng.gen_range(0..ORDERS);
        let wh_idx = rng.gen_range(0..WAREHOUSES);
        let ret_idx = rng.gen_range(0..RETAILERS);
        let dist_idx = rng.gen_range(0..DISTRIBUTORS);
        let year = rng.gen_range(2020..2026);
        let month = rng.gen_range(1..=12);
        let day = rng.gen_range(1..=28);

        entities.push(json!({
            "@id": format!("ex:ship_{u}_{i}"),
            "@type": "ex:Shipment",
            "ex:shipDate": format!("{year:04}-{month:02}-{day:02}"),
            "ex:status": pick(&mut rng, SHIPMENT_STATUSES),
            "ex:trackingCode": format!("TRK-{u:04}-{i:06}"),
            "ex:order": {"@id": format!("ex:ord_{u}_{ord_idx}")},
            "ex:from": {"@id": format!("ex:wh_{u}_{wh_idx}")},
            "ex:to": {"@id": format!("ex:ret_{u}_{ret_idx}")},
            "ex:carrier": {"@id": format!("ex:dist_{u}_{dist_idx}")}
        }));
    }

    json!(entities)
}

/// Generate a batch of entities from a unit, sliced by offset and count.
///
/// Returns a JSON-LD insert body with `@context` and `@graph`.
pub fn generate_batch(unit_id: usize, offset: usize, count: usize) -> Value {
    let all = generate_unit(unit_id);
    let arr = all.as_array().expect("unit must be an array");
    let end = (offset + count).min(arr.len());
    let slice = &arr[offset..end];

    json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "@graph": slice
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_unit_entity_count() {
        let unit = generate_unit(0);
        let arr = unit.as_array().unwrap();
        assert_eq!(arr.len(), ENTITIES_PER_UNIT);
    }

    #[test]
    fn test_deterministic() {
        let a = generate_unit(42);
        let b = generate_unit(42);
        assert_eq!(a, b, "Same unit_id must produce identical output");
    }

    #[test]
    fn test_different_units_differ() {
        let a = generate_unit(0);
        let b = generate_unit(1);
        assert_ne!(a, b, "Different unit_ids should produce different output");
    }

    #[test]
    fn test_generate_batch_has_context() {
        let batch = generate_batch(0, 0, 100);
        assert!(batch.get("@context").is_some());
        assert!(batch.get("@graph").is_some());
        let graph = batch["@graph"].as_array().unwrap();
        assert_eq!(graph.len(), 100);
    }

    #[test]
    fn test_generate_batch_bounds() {
        let batch = generate_batch(0, ENTITIES_PER_UNIT - 10, 100);
        let graph = batch["@graph"].as_array().unwrap();
        assert_eq!(graph.len(), 10, "Should clamp to remaining entities");
    }
}
