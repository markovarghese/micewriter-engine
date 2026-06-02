//! Single source of truth for `FieldDef.field_type` → Arrow + Iceberg type mapping.
//!
//! Both `flush_engine` (Arrow schema for the JSON→Parquet writer) and
//! `iceberg_writer` (Iceberg schema for the catalog commit) consume the same
//! `FieldDef` list, so they MUST resolve identical types for every field.
//! Keeping the mapping here prevents the two modules from drifting.

use arrow::datatypes::{DataType, Field, TimeUnit};
use iceberg::spec::{PrimitiveType, Type};
use tracing::warn;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappedType {
    String,
    Long,
    Int,
    Double,
    Float,
    Boolean,
    TimestampTz,
    Timestamp,
    Date,
    Binary,
    List(Box<MappedType>),
}

impl MappedType {
    /// Parse a `FieldDef.field_type` string. Returns `None` for unknown types
    /// so callers can decide whether to fall back or hard-fail.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "string" => Some(Self::String),
            "long" | "int64" => Some(Self::Long),
            "int" | "int32" => Some(Self::Int),
            "double" | "float64" => Some(Self::Double),
            "float" | "float32" => Some(Self::Float),
            "boolean" => Some(Self::Boolean),
            "timestamptz" => Some(Self::TimestampTz),
            "timestamp" => Some(Self::Timestamp),
            "date" => Some(Self::Date),
            "binary" | "bytes" => Some(Self::Binary),
            _ if s.starts_with("list(") && s.ends_with(")") => {
                let inner_s = &s[5..s.len() - 1];
                Self::from_str(inner_s).map(|t| Self::List(Box::new(t)))
            }
            _ => None,
        }
    }

    /// Resolve `s` to a `MappedType`, logging a warning and defaulting to
    /// `String` on unknown values so a typo never silently corrupts a column
    /// type without leaving an audit trail.
    pub fn from_str_or_string(s: &str, field_name: &str) -> Self {
        match Self::from_str(s) {
            Some(t) => t,
            None => {
                warn!(field = field_name, raw_type = s, "Unknown field type — defaulting to String");
                Self::String
            }
        }
    }

    pub fn to_arrow(self) -> DataType {
        match self {
            Self::String => DataType::Utf8,
            Self::Long => DataType::Int64,
            Self::Int => DataType::Int32,
            Self::Double => DataType::Float64,
            Self::Float => DataType::Float32,
            Self::Boolean => DataType::Boolean,
            // arrow-json only accepts offset-based timezones unless the
            // chrono-tz feature is enabled, so use "+00:00" instead of "UTC".
            Self::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Date => DataType::Date32,
            Self::Binary => DataType::Binary,
            Self::List(inner) => DataType::List(Arc::new(Field::new("item", inner.to_arrow(), true))),
        }
    }

    pub fn to_iceberg(self, next_id: &mut i32) -> Type {
        match self {
            Self::String => Type::Primitive(PrimitiveType::String),
            Self::Long => Type::Primitive(PrimitiveType::Long),
            Self::Int => Type::Primitive(PrimitiveType::Int),
            Self::Double => Type::Primitive(PrimitiveType::Double),
            Self::Float => Type::Primitive(PrimitiveType::Float),
            Self::Boolean => Type::Primitive(PrimitiveType::Boolean),
            Self::TimestampTz => Type::Primitive(PrimitiveType::Timestamptz),
            Self::Timestamp => Type::Primitive(PrimitiveType::Timestamp),
            Self::Date => Type::Primitive(PrimitiveType::Date),
            Self::Binary => Type::Primitive(PrimitiveType::Binary),
            Self::List(inner) => {
                let inner_type = inner.to_iceberg(next_id);
                let element_id = *next_id;
                *next_id += 1;
                Type::List(iceberg::spec::ListType {
                    element_field: std::sync::Arc::new(iceberg::spec::NestedField::optional(element_id, "element", inner_type)),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_aliases_resolve() {
        let pairs = [
            ("string", MappedType::String),
            ("long", MappedType::Long),
            ("int", MappedType::Int),
            ("double", MappedType::Double),
            ("float", MappedType::Float),
            ("boolean", MappedType::Boolean),
            ("timestamptz", MappedType::TimestampTz),
            ("timestamp", MappedType::Timestamp),
            ("date", MappedType::Date),
            ("binary", MappedType::Binary),
            ("list(double)", MappedType::List(Box::new(MappedType::Double))),
            ("list(string)", MappedType::List(Box::new(MappedType::String))),
        ];
        for (s, expected) in pairs {
            assert_eq!(MappedType::from_str(s), Some(expected), "primary alias '{}' failed", s);
        }
    }

    #[test]
    fn secondary_aliases_resolve() {
        // Aliases the Iceberg side already accepted; the Arrow side used to
        // silently fall through to Utf8 for these before consolidation.
        let pairs = [
            ("int64", MappedType::Long),
            ("int32", MappedType::Int),
            ("float64", MappedType::Double),
            ("float32", MappedType::Float),
            ("bytes", MappedType::Binary),
        ];
        for (s, expected) in pairs {
            assert_eq!(MappedType::from_str(s), Some(expected), "alias '{}' failed", s);
        }
    }

    #[test]
    fn unknown_type_returns_none() {
        assert_eq!(MappedType::from_str("strnig"), None);
        assert_eq!(MappedType::from_str(""), None);
        assert_eq!(MappedType::from_str("uuid"), None);
    }

    #[test]
    fn unknown_type_defaults_to_string() {
        assert_eq!(MappedType::from_str_or_string("strnig", "field_a"), MappedType::String);
    }

    #[test]
    fn arrow_and_iceberg_agree_on_every_variant() {
        // If a new variant is added without updating both to_arrow and
        // to_iceberg, the match arms will fail to compile. This test exists
        // to make the per-variant intent explicit.
        let all = [
            MappedType::String,
            MappedType::Long,
            MappedType::Int,
            MappedType::Double,
            MappedType::Float,
            MappedType::Boolean,
            MappedType::TimestampTz,
            MappedType::Timestamp,
            MappedType::Date,
            MappedType::Binary,
            MappedType::List(Box::new(MappedType::Double)),
        ];
        for t in all {
            // Just call both — failures here mean the impl panicked.
            let _ = t.to_arrow();
            let mut next_id = 1;
            let _ = t.to_iceberg(&mut next_id);
        }
    }

    #[test]
    fn timestamptz_uses_offset_not_named_zone() {
        // arrow-json (without chrono-tz) rejects "UTC". Guard against
        // accidentally regressing to a named timezone.
        match MappedType::TimestampTz.to_arrow() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert!(tz.starts_with('+') || tz.starts_with('-'),
                    "expected offset timezone, got '{}'", tz);
            }
            other => panic!("unexpected arrow type for timestamptz: {:?}", other),
        }
    }
}
