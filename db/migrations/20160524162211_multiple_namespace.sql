-- Copyright 2015 clair authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- +goose Up

CREATE TABLE IF NOT EXISTS Namespace (
  id SERIAL PRIMARY KEY,
  name VARCHAR(128) NULL);

-- -----------------------------------------------------
-- Update Namespace data
-- -----------------------------------------------------
ALTER TABLE Namespace ADD version VARCHAR(128) NULL;

UPDATE Namespace SET version = split_part(Namespace.Name, ':', 2), name = split_part(Namespace.Name,':', 1);

-- -----------------------------------------------------
-- Table LayerNamespace
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS LayerNamespace (
        id SERIAL PRIMARY KEY,
	layer_id INT NOT NULL REFERENCES Layer ON DELETE CASCADE,
        namespace_id INT NOT NULL REFERENCES Namespace ON DELETE CASCADE,
        UNIQUE (layer_id, namespace_id));
CREATE INDEX ON LayerNamespace (layer_id);
CREATE INDEX ON LayerNamespace (layer_id, namespace_id);

-- -----------------------------------------------------
-- Update LayerNamespace data
-- -----------------------------------------------------
INSERT INTO LayerNamespace(layer_id, namespace_id) 
	SELECT id, namespace_id
	from Layer;

ALTER TABLE Layer DROP COLUMN namespace_id;

-- +goose Down
ALTER TABLE Layer ADD namespace_id INT NULL REFERENCES Namespace;
                          CREATE INDEX ON Layer (namespace_id);

UPDATE Layer SET (namespace_id) =
 (SELECT namespace_id FROM LayerNamespace
WHERE Layer.id = LayerNamespace.layer_id LIMIT 1);

DROP TABLE IF EXISTS LayerNamespace
            CASCADE;

UPDATE Namespace n SET n.namespace = concat(n.namespace, ':',  n.version);

ALTER TABLE Namespace DROP COLUMN version;
