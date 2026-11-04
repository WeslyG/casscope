CREATE KEYSPACE IF NOT EXISTS lab_smoke WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
CREATE TABLE IF NOT EXISTS lab_smoke.probe (id int PRIMARY KEY, value text);
