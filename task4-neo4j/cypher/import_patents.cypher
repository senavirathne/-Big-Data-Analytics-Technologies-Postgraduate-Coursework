CREATE CONSTRAINT patent_id_unique IF NOT EXISTS
FOR (patent:Patent)
REQUIRE patent.id IS UNIQUE;

MATCH (patent:Patent)
DETACH DELETE patent;

LOAD CSV WITH HEADERS FROM 'file:///patent_edges_5000.csv' AS row
WITH toInteger(row.source) AS source_id, toInteger(row.target) AS target_id
MERGE (source:Patent {id: source_id})
MERGE (target:Patent {id: target_id})
CREATE (source)-[:CITES]->(target);
