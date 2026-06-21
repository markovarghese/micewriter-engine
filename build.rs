use std::env;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../micewriter-sdk-java/micewriter-sdk-java-core/src/main/proto/micewriter.proto";
    
    // Tell cargo to recompile if the proto file changes.
    println!("cargo:rerun-if-changed={}", proto_file);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&[proto_file], &["../micewriter-sdk-java/micewriter-sdk-java-core/src/main/proto"])?;

    // Schema Codegen
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("schema_codegen.rs");
    let schemas_dir = Path::new("schemas");

    println!("cargo:rerun-if-changed=schemas");

    let mut generated_code = String::from("use serde::{Deserialize, Serialize};\n\n");

    if schemas_dir.exists() {
        for entry in fs::read_dir(schemas_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let file_name = path.file_stem().unwrap().to_str().unwrap();
                let struct_name = to_pascal_case(file_name);
                
                let content = fs::read_to_string(&path)?;
                let schema: serde_json::Value = serde_json::from_str(&content)?;
                
                generated_code.push_str(&generate_struct(&struct_name, &schema));
            }
        }
    } else {
        // Fallback for tests if schemas dir doesn't exist yet
        generated_code.push_str(
r#"#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub id: String,
    pub source: Option<String>,
    pub occurred_at: Option<i64>,
}
"#
        );
    }

    fs::write(dest_path, generated_code)?;

    Ok(())
}

fn to_pascal_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize = true;
    for c in s.chars() {
        if c == '_' || c == '-' {
            capitalize = true;
        } else if capitalize {
            result.push(c.to_ascii_uppercase());
            capitalize = false;
        } else {
            result.push(c);
        }
    }
    result
}

fn generate_struct(name: &str, schema: &serde_json::Value) -> String {
    let mut out = format!("#[derive(Debug, Clone, Serialize, Deserialize)]\npub struct {} {{\n", name);
    
    if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            let field_name = field["name"].as_str().unwrap_or("unknown");
            let required = field["required"].as_bool().unwrap_or(false);
            let field_type = parse_type(&field["type"]);
            
            let final_type = if required {
                field_type
            } else {
                format!("Option<{}>", field_type)
            };
            
            out.push_str(&format!("    pub {}: {},\n", field_name, final_type));
        }
    }
    
    out.push_str("}\n\n");
    out
}

fn parse_type(type_val: &serde_json::Value) -> String {
    if let Some(s) = type_val.as_str() {
        match s {
            "string" => "String".to_string(),
            "long" => "i64".to_string(),
            "int" => "i32".to_string(),
            "boolean" => "bool".to_string(),
            "float" => "f32".to_string(),
            "double" => "f64".to_string(),
            _ => "String".to_string(), // fallback
        }
    } else if let Some(obj) = type_val.as_object() {
        if let Some(t) = obj.get("type").and_then(|t| t.as_str()) {
            if t == "list" {
                let elem_type = parse_type(&obj["element"]);
                return format!("Vec<{}>", elem_type);
            }
        }
        "serde_json::Value".to_string() // unsupported nested structures fallback to generic Value
    } else {
        "String".to_string()
    }
}

