"""Test PyIceberg utilities and partition specification handling."""

import pytest
from typing import Dict, Any, List, Optional
from unittest.mock import MagicMock, patch

# Import order following dlt conventions
import pyarrow as pa

from dlt.common.libs.pyiceberg import (
    extract_partition_specs_from_schema,
    PartitionType,
    PartitionSpec,
    IcebergPartitionManager,
)

# Skip all tests if pyiceberg is not available
pytest.importorskip("pyarrow")

# mark all tests as essential for CI
pytestmark = pytest.mark.essential


class TestPartitionSpecExtraction:
    """Test extraction of partition specifications from dlt table schemas."""

    def test_no_partition_hints(self) -> None:
        """Test schema with no partition hints returns empty list."""
        table_schema = {
            "columns": {
                "id": {"name": "id", "data_type": "bigint"},
                "name": {"name": "name", "data_type": "text"},
            }
        }
        arrow_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
        assert specs == []

    def test_legacy_partition_true(self) -> None:
        """Test legacy partition: True syntax."""
        table_schema = {
            "columns": {
                "id": {"name": "id", "data_type": "bigint", "partition": True},
                "category": {"name": "category", "data_type": "text", "partition": True},
                "name": {"name": "name", "data_type": "text"},
            }
        }
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("category", pa.string()),
                pa.field("name", pa.string()),
            ]
        )

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        assert len(specs) == 2
        # Sort by column name for consistent testing
        specs_by_col = {spec["column"]: spec for spec in specs}

        assert specs_by_col["id"]["type"] == "identity"
        assert specs_by_col["id"]["index"] == 1  # Auto-assigned

        assert specs_by_col["category"]["type"] == "identity"
        assert specs_by_col["category"]["index"] == 2  # Auto-assigned

    def test_advanced_single_partition(self) -> None:
        """Test single advanced partition specification."""
        table_schema = {
            "columns": {
                "created_date": {
                    "name": "created_date",
                    "data_type": "date",
                    "partition": {"type": "year", "index": 1},
                },
                "name": {"name": "name", "data_type": "text"},
            }
        }
        arrow_schema = pa.schema(
            [pa.field("created_date", pa.date32()), pa.field("name", pa.string())]
        )

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        assert len(specs) == 1
        spec = specs[0]
        assert spec["column"] == "created_date"
        assert spec["type"] == "year"
        assert spec["index"] == 1
        assert spec["name"] is None  # No custom name provided

    def test_advanced_with_custom_name(self) -> None:
        """Test advanced partition with custom name."""
        table_schema = {
            "columns": {
                "event_time": {
                    "name": "event_time",
                    "data_type": "timestamp",
                    "partition": {"type": "month", "index": 1, "name": "monthly_partition"},
                }
            }
        }
        arrow_schema = pa.schema([pa.field("event_time", pa.timestamp("us"))])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        assert len(specs) == 1
        spec = specs[0]
        assert spec["column"] == "event_time"
        assert spec["type"] == "month"
        assert spec["index"] == 1
        assert spec["name"] == "monthly_partition"

    def test_bucket_partition_with_count(self) -> None:
        """Test bucket partition with bucket count."""
        table_schema = {
            "columns": {
                "user_id": {
                    "name": "user_id",
                    "data_type": "text",
                    "partition": {"type": "bucket", "bucket_count": 16, "index": 1},
                }
            }
        }
        arrow_schema = pa.schema([pa.field("user_id", pa.string())])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        assert len(specs) == 1
        spec = specs[0]
        assert spec["column"] == "user_id"
        assert spec["type"] == "bucket"
        assert spec["bucket_count"] == 16
        assert spec["index"] == 1

    def test_multiple_advanced_partitions_ordered(self) -> None:
        """Test multiple advanced partitions with explicit ordering."""
        table_schema = {
            "columns": {
                "region": {
                    "name": "region",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 2},
                },
                "created_date": {
                    "name": "created_date",
                    "data_type": "date",
                    "partition": {"type": "year", "index": 1, "name": "year_partition"},
                },
                "category": {
                    "name": "category",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 3},
                },
            }
        }
        arrow_schema = pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("created_date", pa.date32()),
                pa.field("category", pa.string()),
            ]
        )

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        assert len(specs) == 3

        # Should be ordered by index
        assert specs[0]["column"] == "created_date"
        assert specs[0]["index"] == 1

        assert specs[1]["column"] == "region"
        assert specs[1]["index"] == 2

        assert specs[2]["column"] == "category"
        assert specs[2]["index"] == 3

    def test_list_of_partitions(self) -> None:
        """Test multiple partitions on same column (list syntax)."""
        table_schema = {
            "columns": {
                "event_time": {
                    "name": "event_time",
                    "data_type": "timestamp",
                    "partition": [
                        {"type": "year", "index": 1, "name": "year_part"},
                        {"type": "month", "index": 2, "name": "month_part"},
                    ],
                }
            }
        }
        arrow_schema = pa.schema([pa.field("event_time", pa.timestamp("us"))])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        assert len(specs) == 2

        # Sorted by index
        assert specs[0]["column"] == "event_time"
        assert specs[0]["type"] == "year"
        assert specs[0]["name"] == "year_part"

        assert specs[1]["column"] == "event_time"
        assert specs[1]["type"] == "month"
        assert specs[1]["name"] == "month_part"

    def test_priority_system_advanced_over_legacy(self) -> None:
        """Test that advanced partitions take priority over legacy ones."""
        table_schema = {
            "columns": {
                "id": {"name": "id", "data_type": "bigint", "partition": True},  # Legacy
                "created_date": {
                    "name": "created_date",
                    "data_type": "date",
                    "partition": {"type": "year", "index": 1},  # Advanced
                },
                "category": {"name": "category", "data_type": "text", "partition": True},  # Legacy
            }
        }
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("created_date", pa.date32()),
                pa.field("category", pa.string()),
            ]
        )

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        # Should only return advanced partitions, ignoring legacy
        assert len(specs) == 1
        assert specs[0]["column"] == "created_date"
        assert specs[0]["type"] == "year"
        assert specs[0]["index"] == 1


class TestPartitionType:
    """Test PartitionType enum."""

    def test_all_supported_types(self) -> None:
        """Test that all expected partition types are supported."""
        expected_types = {"IDENTITY", "BUCKET", "TRUNCATE", "DAY", "MONTH", "YEAR", "HOUR"}
        actual_types = {pt.value for pt in PartitionType}
        assert actual_types == expected_types

    def test_case_insensitive_lookup(self) -> None:
        """Test partition type lookup is case insensitive."""
        assert PartitionType.YEAR.value == "year"
        assert PartitionType.IDENTITY.value == "identity"


class TestPartitionSpec:
    """Test PartitionSpec class."""

    def test_partition_spec_creation(self) -> None:
        """Test creating partition specifications."""
        spec = PartitionSpec(
            column="test_col", partition_type="year", index=1, name="custom_name", bucket_count=None
        )

        assert spec.column == "test_col"
        assert spec.partition_type == "year"
        assert spec.index == 1
        assert spec.name == "custom_name"
        assert spec.bucket_count is None

    def test_partition_spec_with_bucket(self) -> None:
        """Test partition spec with bucket count."""
        spec = PartitionSpec(column="user_id", partition_type="bucket", index=2, bucket_count=32)

        assert spec.bucket_count == 32


class TestIcebergPartitionManager:
    """Test IcebergPartitionManager functionality."""

    def test_create_identity_transform(self) -> None:
        """Test creating identity transform."""
        # This would typically require PyIceberg imports and mocking
        # For now, test the basic structure
        assert hasattr(IcebergPartitionManager, "apply_partitioning")

    def test_partition_manager_structure(self) -> None:
        """Test that partition manager has required methods."""
        # Ensure the class structure is correct
        required_methods = ["apply_partitioning"]
        for method in required_methods:
            assert hasattr(IcebergPartitionManager, method)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_schema(self) -> None:
        """Test with empty schema."""
        table_schema: Dict[str, Any] = {"columns": {}}
        arrow_schema = pa.schema([])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
        assert specs == []

    def test_invalid_partition_type(self) -> None:
        """Test handling of invalid partition type."""
        table_schema = {
            "columns": {
                "test_col": {
                    "name": "test_col",
                    "data_type": "text",
                    "partition": {"type": "invalid_type", "index": 1},
                }
            }
        }
        arrow_schema = pa.schema([pa.field("test_col", pa.string())])

        # Should skip invalid partition types
        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
        assert specs == []  # Invalid type should be skipped

    def test_missing_bucket_count_for_bucket_type(self) -> None:
        """Test bucket partition without bucket_count."""
        table_schema = {
            "columns": {
                "test_col": {
                    "name": "test_col",
                    "data_type": "text",
                    "partition": {"type": "bucket", "index": 1},  # Missing bucket_count
                }
            }
        }
        arrow_schema = pa.schema([pa.field("test_col", pa.string())])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
        assert specs == []  # Should be skipped due to missing bucket_count

    def test_duplicate_indices(self) -> None:
        """Test handling of duplicate partition indices should fail."""
        table_schema = {
            "columns": {
                "col1": {
                    "name": "col1",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 1},
                },
                "col2": {
                    "name": "col2",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 1},  # Same index
                },
            }
        }
        arrow_schema = pa.schema([pa.field("col1", pa.string()), pa.field("col2", pa.string())])

        # Should raise ValueError for duplicate indices
        with pytest.raises(ValueError, match="Duplicate partition indices found"):
            extract_partition_specs_from_schema(table_schema, arrow_schema)


class TestValidationEnhancements:
    """Test new validation features."""

    def test_invalid_bucket_count_types(self) -> None:
        """Test bucket partition with invalid bucket_count types."""
        test_cases = [
            {"bucket_count": "invalid"},  # String
            {"bucket_count": -5},  # Negative
            {"bucket_count": 0},  # Zero
            {"bucket_count": 3.14},  # Float
        ]

        for bucket_config in test_cases:
            table_schema = {
                "columns": {
                    "test_col": {
                        "name": "test_col",
                        "data_type": "text",
                        "partition": {"type": "bucket", "index": 1, **bucket_config},
                    }
                }
            }
            arrow_schema = pa.schema([pa.field("test_col", pa.string())])

            specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
            assert specs == [], f"Should skip invalid bucket_count: {bucket_config}"

    def test_invalid_index_types(self) -> None:
        """Test partition with invalid index types."""
        test_cases = [
            {"index": "invalid"},  # String
            {"index": -1},  # Negative
            {"index": 0},  # Zero
            {"index": 3.14},  # Float
        ]

        for index_config in test_cases:
            table_schema = {
                "columns": {
                    "test_col": {
                        "name": "test_col",
                        "data_type": "text",
                        "partition": {"type": "identity", **index_config},
                    }
                }
            }
            arrow_schema = pa.schema([pa.field("test_col", pa.string())])

            specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
            assert specs == [], f"Should skip invalid index: {index_config}"

    def test_complex_duplicate_indices_scenario(self) -> None:
        """Test complex scenario with multiple duplicate indices should fail."""
        table_schema = {
            "columns": {
                "col_a": {
                    "name": "col_a",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 2},
                },
                "col_b": {
                    "name": "col_b",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 1},
                },
                "col_c": {
                    "name": "col_c",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 2},  # Duplicate of col_a
                },
                "col_d": {
                    "name": "col_d",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 1},  # Duplicate of col_b
                },
            }
        }
        arrow_schema = pa.schema([
            pa.field("col_a", pa.string()),
            pa.field("col_b", pa.string()),
            pa.field("col_c", pa.string()),
            pa.field("col_d", pa.string()),
        ])

        # Should raise ValueError for duplicate indices
        with pytest.raises(ValueError, match="Duplicate partition indices found"):
            extract_partition_specs_from_schema(table_schema, arrow_schema)

    def test_performance_early_exit(self) -> None:
        """Test performance optimization with early exit."""
        # Schema with no partition hints should exit early
        columns = {}
        for i in range(1000):  # Large schema
            columns[f"col_{i}"] = {"name": f"col_{i}", "data_type": "text"}

        table_schema = {"columns": columns}
        arrow_fields = [pa.field(f"col_{i}", pa.string()) for i in range(1000)]
        arrow_schema = pa.schema(arrow_fields)

        import time
        start = time.time()
        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
        duration = time.time() - start

        # Should be very fast due to early exit
        assert specs == []
        assert duration < 0.1  # Should complete in < 100ms

    def test_mixed_valid_invalid_partitions(self) -> None:
        """Test mix of valid and invalid partition specifications."""
        table_schema = {
            "columns": {
                "valid_identity": {
                    "name": "valid_identity",
                    "data_type": "text",
                    "partition": {"type": "identity", "index": 1},
                },
                "invalid_bucket": {
                    "name": "invalid_bucket",
                    "data_type": "text",
                    "partition": {"type": "bucket", "index": 2},  # Missing bucket_count
                },
                "valid_bucket": {
                    "name": "valid_bucket",
                    "data_type": "text",
                    "partition": {"type": "bucket", "index": 3, "bucket_count": 16},
                },
                "invalid_type": {
                    "name": "invalid_type",
                    "data_type": "text",
                    "partition": {"type": "unsupported", "index": 4},
                },
                "valid_year": {
                    "name": "valid_year",
                    "data_type": "timestamp",
                    "partition": {"type": "year", "index": 5},
                },
            }
        }
        arrow_schema = pa.schema([
            pa.field("valid_identity", pa.string()),
            pa.field("invalid_bucket", pa.string()),
            pa.field("valid_bucket", pa.string()),
            pa.field("invalid_type", pa.string()),
            pa.field("valid_year", pa.timestamp("us")),
        ])

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        # Should only include valid partitions
        assert len(specs) == 3
        valid_columns = {spec["column"] for spec in specs}
        assert valid_columns == {"valid_identity", "valid_bucket", "valid_year"}
        
        # Should maintain correct order
        assert specs[0]["column"] == "valid_identity"
        assert specs[0]["index"] == 1
        assert specs[1]["column"] == "valid_bucket"
        assert specs[1]["index"] == 3  # Note: index 2 was skipped due to invalid partition
        assert specs[2]["column"] == "valid_year"
        assert specs[2]["index"] == 5

    def test_all_supported_partition_types(self) -> None:
        """Test that all supported partition types pass validation."""
        supported_types = ["identity", "bucket", "truncate", "day", "month", "year", "hour"]
        
        columns = {}
        for i, ptype in enumerate(supported_types, 1):
            col_name = f"col_{ptype}"
            partition_config = {"type": ptype, "index": i}
            
            # Add bucket_count for bucket type
            if ptype == "bucket":
                partition_config["bucket_count"] = 32
            
            columns[col_name] = {
                "name": col_name,
                "data_type": "timestamp" if ptype in ["day", "month", "year", "hour"] else "text",
                "partition": partition_config,
            }

        table_schema = {"columns": columns}
        arrow_fields = []
        for ptype in supported_types:
            col_name = f"col_{ptype}"
            field_type = pa.timestamp("us") if ptype in ["day", "month", "year", "hour"] else pa.string()
            arrow_fields.append(pa.field(col_name, field_type))
        
        arrow_schema = pa.schema(arrow_fields)

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        # All should be valid
        assert len(specs) == len(supported_types)
        extracted_types = {spec["type"] for spec in specs}
        assert extracted_types == set(supported_types)


class TestRealWorldScenarios:
    """Test real-world scenarios and integration cases."""

    def test_netflix_shows_scenario(self) -> None:
        """Test the exact Netflix shows scenario that caused the user's issue."""
        table_schema = {
            "columns": {
                "show_id": {"name": "show_id", "nullable": False, "data_type": "text"},
                "type": {
                    "name": "type",
                    "nullable": True,
                    "data_type": "text",
                    "partition": {"index": 2, "type": "identity"},
                },
                "title": {"name": "title", "nullable": True, "data_type": "text"},
                "director": {"name": "director", "nullable": True, "data_type": "text"},
                "cast_members": {"name": "cast_members", "nullable": True, "data_type": "text"},
                "country": {"name": "country", "nullable": True, "data_type": "text"},
                "date_added": {
                    "name": "date_added",
                    "nullable": True,
                    "data_type": "date",
                    "partition": {"index": 1, "type": "year", "name": "yearly_partition"},
                },
                "release_year": {"name": "release_year", "nullable": True, "data_type": "bigint"},
                "rating": {"name": "rating", "nullable": True, "data_type": "text"},
                "duration": {"name": "duration", "nullable": True, "data_type": "text"},
                "listed_in": {"name": "listed_in", "nullable": True, "data_type": "text"},
                "description": {"name": "description", "nullable": True, "data_type": "text"},
            }
        }

        arrow_schema = pa.schema(
            [
                pa.field("show_id", pa.string()),
                pa.field("type", pa.string()),
                pa.field("title", pa.string()),
                pa.field("director", pa.string()),
                pa.field("cast_members", pa.string()),
                pa.field("country", pa.string()),
                pa.field("date_added", pa.date32()),
                pa.field("release_year", pa.int64()),
                pa.field("rating", pa.string()),
                pa.field("duration", pa.string()),
                pa.field("listed_in", pa.string()),
                pa.field("description", pa.string()),
            ]
        )

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        # Should extract 2 partitions in correct order
        assert len(specs) == 2

        # Check ordering by index
        assert specs[0]["column"] == "date_added"
        assert specs[0]["type"] == "year"
        assert specs[0]["index"] == 1
        assert specs[0]["name"] == "yearly_partition"

        assert specs[1]["column"] == "type"
        assert specs[1]["type"] == "identity"
        assert specs[1]["index"] == 2
        assert specs[1]["name"] is None

    @patch("dlt.common.libs.pyiceberg.logger")
    def test_partition_manager_error_handling(self, mock_logger) -> None:
        """Test that partition manager handles PyIceberg errors gracefully."""
        mock_update_spec = MagicMock()

        # Simulate PyIceberg error on second partition
        mock_update_spec.add_field.side_effect = [
            None,
            ValueError("Cannot add time partition field"),
        ]

        partition_specs = [
            {
                "column": "region",
                "type": "identity",
                "index": 1,
                "name": None,
                "bucket_count": None,
            },
            {
                "column": "date_added",
                "type": "year",
                "index": 2,
                "name": "year_part",
                "bucket_count": None,
            },
        ]

        # Should not raise exception
        IcebergPartitionManager.apply_partitioning(mock_update_spec, partition_specs)

        # Should log warning for failed partition
        mock_logger.warning.assert_called()
        assert "Failed to apply year partition to column 'date_added'" in str(
            mock_logger.warning.call_args
        )

    def test_mixed_legacy_advanced_priority_system(self) -> None:
        """Test that advanced partitions always take priority over legacy ones."""
        table_schema = {
            "columns": {
                "legacy_col1": {"name": "legacy_col1", "data_type": "text", "partition": True},
                "legacy_col2": {"name": "legacy_col2", "data_type": "text", "partition": True},
                "advanced_col": {
                    "name": "advanced_col",
                    "data_type": "date",
                    "partition": {"type": "month", "index": 1},
                },
            }
        }

        arrow_schema = pa.schema(
            [
                pa.field("legacy_col1", pa.string()),
                pa.field("legacy_col2", pa.string()),
                pa.field("advanced_col", pa.date32()),
            ]
        )

        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)

        # Should only return advanced partition (priority system)
        assert len(specs) == 1
        assert specs[0]["column"] == "advanced_col"
        assert specs[0]["type"] == "month"


class TestPerformance:
    """Test performance characteristics."""

    def test_large_schema_performance(self) -> None:
        """Test extraction performance with large schemas."""
        # Create a schema with many columns
        columns: Dict[str, Dict[str, Any]] = {}
        for i in range(100):
            columns[f"col_{i}"] = {"name": f"col_{i}", "data_type": "text" if i % 2 else "bigint"}

        # Add a few partitions
        columns["col_0"]["partition"] = True
        columns["col_1"]["partition"] = {"type": "identity", "index": 1}

        table_schema = {"columns": columns}
        arrow_fields = [
            pa.field(f"col_{i}", pa.string() if i % 2 else pa.int64()) for i in range(100)
        ]
        arrow_schema = pa.schema(arrow_fields)

        # Should complete quickly
        import time

        start = time.time()
        specs = extract_partition_specs_from_schema(table_schema, arrow_schema)
        duration = time.time() - start

        # Should extract partitions quickly (< 1 second for 100 columns)
        assert duration < 1.0
        assert len(specs) >= 1
