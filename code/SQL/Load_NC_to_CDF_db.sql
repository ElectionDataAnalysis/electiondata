-- Postgresql 11
--
 
-- insert generic data for North Carolina
INSERT INTO cdf.GPUnit (name) VALUES ('North Carolina');
WITH ids AS (SELECT g.id AS nc, i.id AS fips FROM cdf.GPUnit g, cdf.IdentifierType i)
    INSERT INTO cdf.ExternalIdentifier(foreign_id,value,identifiertype_id) VALUES ((SELECT nc FROM ids LIMIT 1),'3700000000',(SELECT fips FROM ids LIMIT 1));

-- BTRIM removes both leading and trailing spaces from a string
INSERT INTO cdf.Party (abbreviation) SELECT DISTINCT BTRIM(choice_party) FROM nc.results_pct WHERE choice_party IS NOT NULL AND BTRIM(choice_party) <> '';
