"""
pipeline/lineage.py
Data lineage tracking for the pipeline.

Tracks:
    - Data sources
    - Transformations applied
    - Data quality at each stage
    - Dependencies between datasets
"""

from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import json
import logging

logger = logging.getLogger(__name__)


class TransformationType(Enum):
    """Types of transformations."""
    EXTRACT = "extract"
    CLEAN = "clean"
    ENCODE = "encode"
    AGGREGATE = "aggregate"
    VALIDATE = "validate"
    LOAD = "load"


class DataSource(Enum):
    """Data sources."""
    CSV = "csv"
    API = "api"
    DATABASE = "database"
    STREAM = "stream"


@dataclass
class DataColumn:
    """Represents a data column."""
    name: str
    data_type: str
    nullable: bool = True
    description: str = ""


@dataclass
class DataQuality:
    """Data quality metrics."""
    row_count: int = 0
    null_count: int = 0
    duplicate_count: int = 0
    completeness: float = 0.0
    validity: float = 0.0
    
    def to_dict(self):
        return {
            "row_count": self.row_count,
            "null_count": self.null_count,
            "duplicate_count": self.duplicate_count,
            "completeness": self.completeness,
            "validity": self.validity,
        }


@dataclass
class DataNode:
    """Represents a node in the data lineage."""
    node_id: str
    name: str
    source: DataSource
    transformation: Optional[TransformationType] = None
    columns: List[DataColumn] = field(default_factory=list)
    quality: Optional[DataQuality] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self):
        return {
            "node_id": self.node_id,
            "name": self.name,
            "source": self.source.value,
            "transformation": self.transformation.value if self.transformation else None,
            "columns": [{"name": c.name, "type": c.data_type} for c in self.columns],
            "quality": self.quality.to_dict() if self.quality else None,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class LineageEdge:
    """Represents an edge between data nodes."""
    from_node: str
    to_node: str
    transformation: TransformationType
    description: str = ""
    
    def to_dict(self):
        return {
            "from": self.from_node,
            "to": self.to_node,
            "transformation": self.transformation.value,
            "description": self.description,
        }


class DataLineage:
    """Data lineage tracker."""
    
    def __init__(self):
        self.nodes: Dict[str, DataNode] = {}
        self.edges: List[LineageEdge] = []
        self.pipeline_run_id: Optional[str] = None
    
    def start_pipeline_run(self, run_id: str):
        """Start a new pipeline run."""
        self.pipeline_run_id = run_id
        logger.info(f"Started lineage tracking for run: {run_id}")
    
    def add_node(self, node: DataNode):
        """Add a data node."""
        self.nodes[node.node_id] = node
        logger.info(f"Added lineage node: {node.node_id}")
    
    def add_edge(self, edge: LineageEdge):
        """Add a lineage edge."""
        self.edges.append(edge)
        logger.info(f"Added lineage edge: {edge.from_node} -> {edge.to_node}")
    
    def get_node(self, node_id: str) -> Optional[DataNode]:
        """Get a node by ID."""
        return self.nodes.get(node_id)
    
    def get_upstream(self, node_id: str) -> List[DataNode]:
        """Get all upstream nodes."""
        upstream = []
        for edge in self.edges:
            if edge.to_node == node_id:
                if edge.from_node in self.nodes:
                    upstream.append(self.nodes[edge.from_node])
        return upstream
    
    def get_downstream(self, node_id: str) -> List[DataNode]:
        """Get all downstream nodes."""
        downstream = []
        for edge in self.edges:
            if edge.from_node == node_id:
                if edge.to_node in self.nodes:
                    downstream.append(self.nodes[edge.to_node])
        return downstream
    
    def get_full_lineage(self) -> Dict:
        """Get full lineage as dictionary."""
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "nodes": {k: v.to_dict() for k, v in self.nodes.items()},
            "edges": [e.to_dict() for e in self.edges],
        }
    
    def export_to_file(self, filepath: str):
        """Export lineage to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(self.get_full_lineage(), f, indent=2)
        logger.info(f"Exported lineage to {filepath}")


# Global lineage instance
lineage = DataLineage()


# Helper functions for tracking
def track_data_node(
    node_id: str,
    name: str,
    source: DataSource,
    transformation: Optional[TransformationType] = None,
    columns: Optional[List[DataColumn]] = None,
    quality: Optional[DataQuality] = None,
):
    """Track a data node in lineage."""
    node = DataNode(
        node_id=node_id,
        name=name,
        source=source,
        transformation=transformation,
        columns=columns or [],
        quality=quality,
    )
    lineage.add_node(node)
    return node


def track_transformation(from_node_id: str, to_node_id: str, transformation: TransformationType, description: str = ""):
    """Track a transformation in lineage."""
    edge = LineageEdge(
        from_node=from_node_id,
        to_node=to_node_id,
        transformation=transformation,
        description=description,
    )
    lineage.add_edge(edge)
    return edge


# Example usage
if __name__ == "__main__":
    # Start pipeline run
    lineage.start_pipeline_run("run_001")
    
    # Track raw data
    raw_node = track_data_node(
        node_id="raw_claims",
        name="Raw Claims Data",
        source=DataSource.CSV,
        transformation=TransformationType.EXTRACT,
    )
    
    # Track cleaned data
    clean_node = track_data_node(
        node_id="clean_claims",
        name="Cleaned Claims Data",
        source=DataSource.CSV,
        transformation=TransformationType.CLEAN,
    )
    
    # Track transformation
    track_transformation(
        from_node_id="raw_claims",
        to_node_id="clean_claims",
        transformation=TransformationType.CLEAN,
        description="Removed nulls, standardized columns",
    )
    
    # Export
    lineage.export_to_file("lineage.json")
    
    # Print
    print(json.dumps(lineage.get_full_lineage(), indent=2))
