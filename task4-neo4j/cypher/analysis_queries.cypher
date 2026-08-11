// Query 1 — Direct-neighbor layer for the explicit target patent 3858514.
// PROFILE records the operator plan, rows, database hits, and elapsed behavior.
PROFILE
MATCH (target:Patent {id: 3858514})-[edge:CITES {
  coursework_dataset: 'snap-cit-Patents-first-5000-v1'
}]-(neighbor:Patent)
RETURN target.id AS target_patent,
       neighbor.id AS direct_neighbor,
       CASE
         WHEN startNode(edge) = target THEN 'OUTGOING_CITES'
         ELSE 'INCOMING_CITED_BY'
       END AS neighbor_direction
ORDER BY direct_neighbor ASC, neighbor_direction ASC;

// Query 2 — In-degree centrality. Stable descending centrality and ascending-ID
// ordering deterministically identifies the ten most-cited patents in the subset.
PROFILE
MATCH (patent:Patent)
WHERE EXISTS {
  MATCH (patent)-[:CITES {
    coursework_dataset: 'snap-cit-Patents-first-5000-v1'
  }]-()
}
OPTIONAL MATCH (:Patent)-[incoming:CITES {
  coursework_dataset: 'snap-cit-Patents-first-5000-v1'
}]->(patent)
WITH patent, count(incoming) AS in_degree
ORDER BY in_degree DESC, patent.id ASC
LIMIT 10
RETURN patent.id AS patent_id, in_degree
ORDER BY in_degree DESC, patent_id ASC;

// Query 3 — Shortest path between two explicit distant patents. In the exact
// first-5,000-edge SNAP subset these endpoints are six undirected citation hops
// apart, so the result maps a hidden linkage rather than a direct-neighbor edge.
// PROFILE documents traversal behavior.
PROFILE
MATCH (start:Patent {id: 3484134}), (finish:Patent {id: 253889})
MATCH path = ANY SHORTEST (start)-[:CITES {
  coursework_dataset: 'snap-cit-Patents-first-5000-v1'
}]-{1,15}(finish)
RETURN start.id AS start_patent,
       finish.id AS finish_patent,
       length(path) AS hop_count,
       [node IN nodes(path) | node.id] AS patent_path;
