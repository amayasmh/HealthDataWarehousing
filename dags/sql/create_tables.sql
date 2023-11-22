-- Supprimer la table urgence_hospit si elle existe
DROP TABLE IF EXISTS urgence_hospit;

-- Supprimer la table urgence_mesures si elle existe
DROP TABLE IF EXISTS urgence_mesures;

-- Supprimer la table tranche_age si elle existe
DROP TABLE IF EXISTS tranche_age;

-- Supprimer la table departement si elle existe
DROP TABLE IF EXISTS departement;



CREATE TABLE IF NOT EXISTS departement (
    id_dep VARCHAR(5) PRIMARY KEY,
    nom_dep VARCHAR(255),
    region VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS tranche_age (
    id INT PRIMARY KEY,
    tranche VARCHAR(25)

);

CREATE TABLE IF NOT EXISTS urgence_mesures (
    id SERIAL PRIMARY KEY,
    dep VARCHAR(5),
    date_de_passage DATE,
    code_age INT,
    nbre_pass_tot INT,
    nbre_pass_corona INT,
    nbre_hospit_corona INT,
    FOREIGN KEY (dep) REFERENCES departement(id)
    FOREIGN KEY (code_age) REFERENCES tranche_age(id)
);


CREATE TABLE IF NOT EXISTS urgence_hospit (
    id SERIAL PRIMARY KEY,
    urgence_mesures_id INT,
    nbre_pass_tot_h INT,
    nbre_pass_tot_f INT,
    nbre_pass_corona_h INT,
    nbre_pass_corona_f INT,
    nbre_hospit_corona_h INT,
    nbre_hospit_corona_f INT,
    FOREIGN KEY (urgence_mesures_id) REFERENCES urgence_mesures(id)
);