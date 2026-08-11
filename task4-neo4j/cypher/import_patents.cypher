CREATE CONSTRAINT patent_id_unique IF NOT EXISTS
FOR (patent:Patent)
REQUIRE patent.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///patent_edges_5000.csv' AS row
WITH toInteger(row.source) AS source_id,
     toInteger(row.target) AS target_id,
     row.source AS source_text,
     row.target AS target_text
MERGE (source:Patent {id: source_id})
MERGE (target:Patent {id: target_id})
MERGE (source)-[:CITES {
  coursework_dataset: 'snap-cit-Patents-first-5000-v1',
  coursework_edge: 'snap-cit-Patents-first-5000-v1:' + source_text + '->' + target_text
}]->(target);
