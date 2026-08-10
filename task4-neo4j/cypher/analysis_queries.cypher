// Query 1 — Direct-neighbor layer. The target is deterministically the highest
// total-degree patent, with the smallest patent ID breaking ties. PROFILE records
// the operator plan, rows, database hits, and elapsed execution behavior.
PROFILE
MATCH (candidate:Patent)
OPTIONAL MATCH (candidate)-[candidate_edge:CITES]-()
WITH candidate, count(candidate_edge) AS total_degree
ORDER BY total_degree DESC, candidate.id ASC
LIMIT 1
MATCH (candidate)-[edge:CITES]-(neighbor:Patent)
RETURN candidate.id AS target_patent,
       total_degree,
       neighbor.id AS direct_neighbor,
       CASE
         WHEN startNode(edge) = candidate THEN 'OUTGOING_CITES'
         ELSE 'INCOMING_CITED_BY'
       END AS neighbor_direction
ORDER BY direct_neighbor ASC, neighbor_direction ASC;

// Query 2 — In-degree centrality. Stable descending centrality and ascending-ID
// ordering deterministically identifies the ten most-cited patents in the subset.
PROFILE
MATCH (patent:Patent)
OPTIONAL MATCH (:Patent)-[incoming:CITES]->(patent)
WITH patent, count(incoming) AS in_degree
ORDER BY in_degree DESC, patent.id ASC
LIMIT 10
RETURN patent.id AS patent_id, in_degree
ORDER BY in_degree DESC, patent_id ASC;

// Query 3 — Shortest path. The same deterministic hub used by Query 1 is the
// start. The farthest reachable patent within 15 hops is selected, with smallest
// patent ID breaking equal-distance ties. PROFILE documents traversal behavior.
PROFILE
MATCH (candidate:Patent)
OPTIONAL MATCH (candidate)-[candidate_edge:CITES]-()
WITH candidate, count(candidate_edge) AS total_degree
ORDER BY total_degree DESC, candidate.id ASC
LIMIT 1
MATCH path = ANY SHORTEST (candidate)-[:CITES]-{1,15}(finish:Patent)
WHERE finish <> candidate
WITH candidate AS start, finish, path
ORDER BY length(path) DESC, finish.id ASC
LIMIT 1
RETURN start.id AS start_patent,
       finish.id AS finish_patent,
       length(path) AS hop_count,
       [node IN nodes(path) | node.id] AS patent_path;
