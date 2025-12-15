import duckdb

def my_connect(path_file, df_name, requete):
    # Création de la connexion
    con = duckdb.connect(database=':memory:')  # Utilise une base de données en mémoire
    # Création d'une vue vers le fichier Parquet
    con.execute(f"CREATE OR REPLACE VIEW {df_name} AS SELECT * FROM '{path_file}'")
    # Exécution de la requête et récupération des résultats dans un DataFrame
    resultat = con.execute(requete).fetchdf()
    print(resultat)     # Affichage du résultat
    con.close()         # Fermeture de la connexion - ⚠ IMPORTANT ⚠


# Exo1
my_connect(
    path_file='https://static.data.gouv.fr/resources/recensement-de-la-population-fichiers-detail-individus-localises-au-canton-ou-ville-2020-1/20231023-122841/fd-indcvi-2020.parquet', 
    df_name='rp2020',
    requete="""
    SELECT
        AGEREV,
        DIPL,
        MOCO,
        CANTVILLE,
        IPONDI,
        SEXE
    FROM
        rp2020
    LIMIT 5;
"""
)
# Définition de la requête SQL


"""
-- Exercice 2
-- On souhaite créer la table rp2020_guadeloupe des individus résidant en Guadeloupe
-- (DEPT = '971' ) comprenant les variables suivantes :
-- REG, la région codée sur deux caractères à partir de la variable REGION
--   (ajouter un 0 devant);
--  SEXE, le sexe sous la forme ‘Hommes’, ‘Femmes’ ('Hommes' = 1 et 'Femmes' = '2');
--  AGED, l'âge en différence de millésimes ;
--  ANAIS l'année de naissance calculée à partir de AGED.
--  IPONDI nombre d'individus ayant les mêmes caractéristiques (pondération)
-- Classer la table dans l'ordre des années de naissance ANAIS
--  Visualliser les 10 premières lignes

SELECT
	LPAD(REGION, 2, '0') AS REGION,
	CASE
		WHEN SEXE = 1 THEN 'Hommes'
		WHEN SEXE = 2 THEN 'Femmes'
		ELSE 'inconnu'
	END AS SEXE,
	AGED,
	ANAI AS ANAIS,
	2020-AGED AS ANAIS1,
	IPONDI,
FROM
	rp2020
WHERE
	DEPT = '971'
ORDER BY
	ANAIS
LIMIT 10;


-- Exercice 3

CREATE OR REPLACE
VIEW rp2020 AS
FROM
'https://static.data.gouv.fr/resources/recensement-de-la-population-fichiers-detail-individus-localises-au-canton-ou-ville-2020-1/20231023-122841/fd-indcvi-2020.parquet';
-- On souhaite afficher la population des cinq départements du Grand Est (REGION 44) les plus peuplés, classés du plus grand au plus petit. (IPONDI = variable de pondération)
-- On pourra arrondir la population avec la fonction ROUND

SELECT
	DEPT,
	ROUND(SUM(IPONDI))::INT AS POP
FROM
	rp2020
WHERE
	REGION = '44'
GROUP BY
	DEPT
ORDER BY
	POP DESC
LIMIT 5;
-- Exercice 4
-- Reprendre le script de l'exercice 2 (Population par département du Grand Est)
-- Ajouter le nom des départements à l'aide d'une jointure avec la table cog_dep
-- Limiter l'affichage aux départements de moins de 300 000 habitants.
CREATE OR REPLACE
VIEW cog_dep AS
FROM
'https://www.insee.fr/fr/statistiques/fichier/7766585/v_departement_2024.csv';


SELECT
	*
FROM
	cog_dep

SELECT
	REG AS REGION,
	DEPT,
	LIBELLE,
	POP AS POP2013,
FROM
	(
	SELECT
		DEPT,
		ROUND(SUM(IPONDI)) AS POP
	FROM
		rp2020
	WHERE
		REGION = '44'
	GROUP BY
		DEPT) AS A
LEFT JOIN cog_dep ON
	A.DEPT = cog_dep.DEP
WHERE
	POP2013 <= 300000;
-- Equivalent à 
WITH A AS (
SELECT
	DEPT,
	ROUND(SUM(IPONDI)) AS POP
FROM
	rp2020
WHERE
	REGION = '44'
GROUP BY
	DEPT)
SELECT
	REG AS REGION,
	DEPT,
	LIBELLE,
	POP AS POP2013,
FROM
	A
LEFT JOIN cog_dep ON
	A.DEPT = cog_dep.DEP
WHERE
	POP2013 <= 300000;
-- POP2013 c'est pas 2013 l'année ? 
-- Exercice 5
-- Créer la vue pct_cog pointant vers la liste des cantons et des pseudo-cantons issue du code officiel géographique. 
-- En réalisant une jointure entre la table rp2020 et pct_cog, afficher la liste des codes des cantons et pseudos cantons de la table rp2020 n'ayant aucune correspondance avec la table pct_cog.
CREATE OR REPLACE
VIEW pct_cog AS
FROM
'https://www.insee.fr/fr/statistiques/fichier/7766585/v_canton_2024.csv';

SELECT
	*
FROM
	pct_cog

SELECT
	DISTINCT
	-- to remove duplicates
	CANTVILLE
FROM
	rp2020
LEFT OUTER JOIN pct_cog ON
	rp2020.CANTVILLE = pct_cog.CAN
WHERE
	pct_cog.CAN IS NULL;
-- Exercice 6
-- Afficher la liste des départements ayant au moins un habitant né avant 1897.
-- À l'aide d'une jointure ajouter le libellé des départements.
-- On pourra créer la vue cog_dep pointant vers la liste des départements . 
CREATE OR REPLACE
VIEW cog_dep AS
FROM
'https://www.insee.fr/fr/statistiques/fichier/7766585/v_departement_2024.csv';

WITH rp2020_temp AS (
SELECT
	DEPT
FROM
	rp2020
WHERE
	ANAI <= 1897
GROUP BY
	DEPT
HAVING
	ROUND(SUM(IPONDI)) >= 1
)
SELECT
	DISTINCT
  DEPT,
	LIBELLE,
FROM
	rp2020_temp
LEFT JOIN cog_dep ON
	rp2020_temp.DEPT = cog_dep.DEP
ORDER BY
	DEPT ;


-- Exercice 7
CREATE TABLE Stargate (
  prenom VARCHAR(50), 
  nom VARCHAR(50), 
  annee_naissance INT, 
  lieu_naissance VARCHAR(50), 
  pays VARCHAR(50),
  );

INSERT INTO Stargate(prenom, nom, annee_naissance, lieu_naissance, pays)
VALUES 
  ('Michael', 'SHANKS', '1970', 'Vancouver', 'Canada'),
  ('Amanda', 'TAPPING', '1965', 'Rochford', 'Royaume-Uni'),
  ('Teryl', 'ROTHERY', '1962', 'Vancouver', 'Canada'),
  ('Christopher', 'JUDGE', '1964', 'Los Angeles', 'Etats-Unis');

SELECT *
FROM
  Stargate;

INSERT INTO Stargate(prenom, nom, annee_naissance, lieu_naissance, pays)
VALUES 
  ('Richard Dean', 'ANDERSON', '1950', 'Minneapolis', 'Etats-Unis');

-- Ajout de l'age
ALTER TABLE Stargate 
ADD age INT;

UPDATE Stargate
SET age = 2025-annee_naissance;

SELECT *
FROM
  Stargate;
"""
