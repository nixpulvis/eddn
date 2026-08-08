//! Everything that was read before the reader started going by the schema
//!
//! The dispatch was rewritten under these, so what matters is not that they
//! parse -- they always did -- but that they still come out of the same door.
//! Guessing tried each variant in turn and took the first that fitted; this
//! reads the `$schemaRef` and goes straight there. Anything that used to fit
//! by luck rather than by name would fail here.

use eddn::{Envelope, Message};
use elite_journal::entry::incremental::exploration::ScanTarget;
use elite_journal::entry::Event;

fn envelope(schema_ref: &str, message: &str) -> Envelope {
    let json = format!(
        r#"{{
            "$schemaRef": "{}",
            "header": {{
                "gatewayTimestamp": "2026-08-08T12:00:00Z",
                "softwareName": "E:D Market Connector",
                "softwareVersion": "5.11.3",
                "uploaderID": "abc123"
            }},
            "message": {}
        }}"#,
        schema_ref, message,
    );

    serde_json::from_str(&json)
        .unwrap_or_else(|err| panic!("{} should parse: {}", schema_ref, err))
}

fn journal(message: &str) -> Event {
    match envelope("https://eddn.edcd.io/schemas/journal/1", message).message {
        Message::Journal(entry) => entry.event,
        other => panic!("not a journal entry: {:?}", other),
    }
}

/// A star, which is most of what a scan of a system is
#[test]
fn a_star_scan_still_reads() {
    let event = journal(
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "Scan",
            "ScanType": "AutoScan",
            "StarSystem": "Sol",
            "StarPos": [0.0, 0.0, 0.0],
            "SystemAddress": 10477373803,
            "BodyName": "Sol",
            "BodyID": 0,
            "DistanceFromArrivalLS": 0.0,
            "StarType": "G",
            "Subclass": 2,
            "StellarMass": 0.945312,
            "Radius": 696650688.0,
            "AbsoluteMagnitude": 4.83,
            "Age_MY": 4600,
            "SurfaceTemperature": 5778.0,
            "Luminosity": "Va",
            "RotationPeriod": 2211840.0,
            "AxialTilt": 0.126537,
            "WasDiscovered": true,
            "WasMapped": false
        }"#,
    );

    let Event::Scan(scan) = event else { panic!("not a scan") };
    assert_eq!(scan.star_system, "Sol");
    assert!(matches!(scan.target, ScanTarget::Star(_)));
}

/// A planet, which is the other half of the same event
#[test]
fn a_body_scan_still_reads() {
    let event = journal(
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "Scan",
            "ScanType": "Detailed",
            "StarSystem": "Sol",
            "StarPos": [0.0, 0.0, 0.0],
            "SystemAddress": 10477373803,
            "BodyName": "Sol 4",
            "BodyID": 12,
            "Parents": [{"Star": 0}],
            "DistanceFromArrivalLS": 763.7,
            "TidalLock": false,
            "TerraformState": "Terraformable",
            "PlanetClass": "High metal content body",
            "Atmosphere": "thin carbon dioxide atmosphere",
            "AtmosphereType": "CarbonDioxide",
            "Volcanism": "",
            "MassEM": 0.107425,
            "Radius": 3382519.75,
            "SurfaceGravity": 3.740913,
            "SurfaceTemperature": 232.0,
            "SurfacePressure": 700.0,
            "Landable": true,
            "SemiMajorAxis": 227900000000.0,
            "Eccentricity": 0.0934,
            "OrbitalInclination": 1.85,
            "Periapsis": 286.5,
            "OrbitalPeriod": 59354294000.0,
            "AscendingNode": 49.5,
            "MeanAnomaly": 19.4,
            "RotationPeriod": 88642.0,
            "AxialTilt": 0.4396,
            "WasDiscovered": true,
            "WasMapped": true
        }"#,
    );

    let Event::Scan(scan) = event else { panic!("not a scan") };
    assert!(matches!(scan.target, ScanTarget::Body(_)));
}

#[test]
fn docking_still_reads() {
    let event = journal(
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "Docked",
            "StarSystem": "Sol",
            "StarPos": [0.0, 0.0, 0.0],
            "SystemAddress": 10477373803,
            "StationName": "Abraham Lincoln",
            "StationType": "Orbis",
            "MarketID": 128016384,
            "StationFaction": {"Name": "Mother Gaia"},
            "StationGovernment": "$government_Democracy;",
            "StationAllegiance": "Federation",
            "StationServices": ["dock", "refuel", "shipyard"],
            "StationEconomies": [
                {"Name": "$economy_Service;", "Proportion": 1.0}
            ],
            "DistFromStarLS": 498.4
        }"#,
    );

    let Event::Docked(docked) = event else { panic!("not a dock") };
    assert_eq!(docked.station.name, "Abraham Lincoln");
    assert_eq!(docked.system_name, "Sol");
}

#[test]
fn a_jump_still_reads() {
    let event = journal(
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "FSDJump",
            "StarSystem": "Sol",
            "StarPos": [0.0, 0.0, 0.0],
            "SystemAddress": 10477373803,
            "SystemAllegiance": "Federation",
            "SystemEconomy": "$economy_Refinery;",
            "SystemSecondEconomy": "$economy_Service;",
            "SystemGovernment": "$government_Democracy;",
            "SystemSecurity": "$SYSTEM_SECURITY_high;",
            "Population": 22780919531,
            "SystemFaction": {"Name": "Mother Gaia"}
        }"#,
    );

    let Event::FsdJump(jump) = event else { panic!("not a jump") };
    assert_eq!(jump.system.name, "Sol");
    assert_eq!(jump.system.population, Some(22780919531));
}

#[test]
fn arriving_somewhere_still_reads() {
    let event = journal(
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "Location",
            "StarSystem": "Sol",
            "StarPos": [0.0, 0.0, 0.0],
            "SystemAddress": 10477373803,
            "SystemAllegiance": "Federation",
            "SystemEconomy": "$economy_Refinery;",
            "SystemGovernment": "$government_Democracy;",
            "SystemSecurity": "$SYSTEM_SECURITY_high;",
            "Population": 22780919531,
            "Docked": true,
            "StationName": "Abraham Lincoln",
            "StationType": "Orbis",
            "MarketID": 128016384
        }"#,
    );

    let Event::Location(location) = event else { panic!("not a location") };
    assert!(location.docked);
    assert_eq!(
        location.station.as_ref().map(|s| s.name.as_str()),
        Some("Abraham Lincoln"),
    );
}

/// Its own schema, and the payload is a journal event all the same
#[test]
fn a_barycenter_still_reads() {
    let message = envelope(
        "https://eddn.edcd.io/schemas/scanbarycentre/1",
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "ScanBaryCentre",
            "StarSystem": "Sol",
            "StarPos": [0.0, 0.0, 0.0],
            "SystemAddress": 10477373803,
            "BodyID": 31,
            "SemiMajorAxis": 2216000000.0,
            "Eccentricity": 0.0022,
            "OrbitalInclination": 112.8,
            "Periapsis": 34.9,
            "OrbitalPeriod": 551614000.0,
            "AscendingNode": -132.9,
            "MeanAnomaly": 45.2
        }"#,
    )
    .message;

    let Message::Journal(entry) = message else {
        panic!("not a journal entry")
    };
    let Event::ScanBaryCentre(scan) = entry.event else {
        panic!("not a barycenter")
    };
    assert_eq!(scan.body_id, 31);
    assert!(scan.orbit.is_some());
}

#[test]
fn a_nav_route_still_reads() {
    let message = envelope(
        "https://eddn.edcd.io/schemas/navroute/1",
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "NavRoute",
            "Route": [
                {
                    "StarSystem": "Sol",
                    "SystemAddress": 10477373803,
                    "StarPos": [0.0, 0.0, 0.0],
                    "StarClass": "G"
                },
                {
                    "StarSystem": "Alpha Centauri",
                    "SystemAddress": 113718121307,
                    "StarPos": [3.03, -0.09, 3.15],
                    "StarClass": "G"
                }
            ]
        }"#,
    )
    .message;

    let Message::Journal(entry) = message else {
        panic!("not a journal entry")
    };
    assert!(matches!(entry.event, Event::NavRoute(_)));
}

#[test]
fn a_commodity_market_still_reads() {
    let message = envelope(
        "https://eddn.edcd.io/schemas/commodity/3",
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "systemName": "Sol",
            "stationName": "Abraham Lincoln",
            "marketId": 128016384,
            "stationType": "Orbis",
            "economies": [{"name": "Service", "proportion": 1.0}],
            "prohibited": ["Slaves"],
            "commodities": [
                {
                    "name": "gold",
                    "meanPrice": 9411,
                    "buyPrice": 0,
                    "sellPrice": 9432,
                    "demand": 1148,
                    "demandBracket": 2,
                    "stock": 0,
                    "stockBracket": 0
                }
            ]
        }"#,
    )
    .message;

    let Message::Commodity(entry) = message else { panic!("not a market") };
    assert_eq!(entry.event.station_name, "Abraham Lincoln");
    assert_eq!(entry.event.commodities.len(), 1);
}

/// The docking events, which had never been read at all
///
/// `MarketID` is what the game sends and `rename_all = "PascalCase"` makes
/// `MarketId`, a field that does not exist, so every one of these failed on
/// it. `Docked` was never affected: it flattens a `Station`, where the name
/// is spelled out by hand.
///
/// Nothing showed it. A failure here used to be indistinguishable from an
/// event nothing reads, and both were dropped without a word -- which is
/// exactly what going by the `$schemaRef` was meant to expose, since these
/// two have schemas of their own.
#[test]
fn a_docking_permission_reads_with_only_what_the_schema_requires() {
    let message = envelope(
        "https://eddn.edcd.io/schemas/dockinggranted/1",
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "DockingGranted",
            "MarketID": 128016384,
            "StationName": "Abraham Lincoln"
        }"#,
    )
    .message;

    let Message::Journal(entry) = message else {
        panic!("not a journal entry")
    };
    let Event::DockingGranted(granted) = entry.event else {
        panic!("not a docking permission")
    };

    assert_eq!(granted.market_id, 128016384);
    // Both optional in the schema, and absent here.
    assert_eq!(granted.station_type, None);
    assert_eq!(granted.landing_pad, None);
}

#[test]
fn a_docking_refusal_reads_and_says_why() {
    let message = envelope(
        "https://eddn.edcd.io/schemas/dockingdenied/1",
        r#"{
            "timestamp": "2026-08-08T12:00:00Z",
            "event": "DockingDenied",
            "MarketID": 128016384,
            "StationName": "Abraham Lincoln",
            "StationType": "Orbis",
            "Reason": "NoSpace"
        }"#,
    )
    .message;

    let Message::Journal(entry) = message else {
        panic!("not a journal entry")
    };
    let Event::DockingDenied(denied) = entry.event else {
        panic!("not a docking refusal")
    };

    assert_eq!(denied.market_id, 128016384);
    assert_eq!(denied.station_type.as_deref(), Some("Orbis"));
}
