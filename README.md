# HealthDataWarehousing
"HealthDataWarehousing" est un projet visant à établir un entrepôt de données. En utilisant des fichiers sources spécifiques liés aux passages aux urgences pour suspicion de COVID-19, ce projet a pour objectif de répondre à des questions liées à la pandémie.


# Développeurs du Projet

- **Amayas MAHMOUDI**
- **Anis GHEBRIOUA**

## Professeur Encadrant

- **Rahma GARGOURI**


# Dépôt Git

Le code source de ce projet est disponible sur GitHub. Vous pouvez accéder au dépôt en suivant le lien ci-dessous :
[https://github.com/amayasmh/HealthDataWarehousing](https://github.com/amayasmh/HealthDataWarehousing)


# Tâches Réalisées

## 1. Création d'un schéma (fait-dimensions) de l'entrepôt de données
   - Un schéma a été créé pour l'entrepôt de données dans le dossier `Conception`.

## 2. Lecture des fichiers sources
   - Les fichiers sources ont été lus à partir du dossier `data/raw`.

## 3. Transformation des données
   - Les données ont été transformées selon les besoins du projet :
      a. **Sélection des Colonnes Utiles :**
         - Les colonnes pertinentes ont été sélectionnées à partir des jeux de données, incluant uniquement celles nécessaires au projet.
      
      b. **Ajout de Colonnes :**
         - De nouvelles colonnes ont été ajoutées aux DataFrames, comme l'ajout d'une colonne "id" dans le DataFrame `df_urgence_hospit`.

      c. **Renommage des Colonnes :**
         - Les noms de colonnes ont été modifiés pour assurer une meilleure compréhension et cohérence dans les DataFrames.

      d. **Suppression des Lignes avec des Valeurs Nulles :**
         - Les lignes contenant des valeurs nulles ont été supprimées des DataFrames pour garantir la qualité des données.

## 4. Création des tables de l'entrepôt
   - Les tables nécessaires pour l'entrepôt de données ont été créées.
   - Un fichier `create_tables.sql` a été créé, contenant les requêtes pour créer les différentes tables, dans le dossier `dags/sql`.

## 5. Stockage dans des fichiers CSV
   - Les données transformées ont été stockées dans des fichiers CSV dans le dossier `data/processed`.

## 6. Insertion de l'ensemble des données
   - Les données transformées ont été insérées dans les tables de l'entrepôt.

## 7. Ajout d'un notebook pour la visualisation
   - Un notebook a été ajouté dans le dossier `visualisation` pour permettre la visualisation des résultats une fois que le DAG a terminé son exécution.



# Prérequis

Avant de commencer à utiliser ce projet, assurez-vous de disposer des éléments suivants installés sur votre système :

1. **Docker :**
   - [Installer Docker](https://docs.docker.com/get-docker/) sur votre machine.

2. **Python :**
   - [Installer Python](https://www.python.org/downloads/) (version recommandée : Python 3.x).

3. **DBeaver :**
   - [Installer DBeaver](https://dbeaver.io/download/) pour une gestion facile de la base de données.


# Instructions d'Utilisation

Suivez ces instructions pour lancer le projet :

1. **Créer un dossier "processed" à l'intérieur du dossier data :**
   - Assurez-vous de créer un dossier nommé "processed" à l'intérieur du dossier `data`.

2. **Lancer DBeaver :**
   - Démarrez DBeaver et créez une nouvelle connexion en suivant les instructions fournies dans l'image 1 du fichier `support.pdf`.

3. **Ajouter une variable d'environnement :**
   - Sur Linux :
     ```bash
     export airflow_uid=1000
     source ~/.bashrc
     ```
   - Sur Windows :
     ```bash
     set airflow_uid=1000
     ```

4. **Vérifier la disponibilité des ports :**
   - Assurez-vous que le port 5435 est disponible. Libérez-le si nécessaire.
   - Assurez-vous également que le port 8081 est disponible.
   
5. **Arrêter les conteneurs déjà lancés :**
   - Fermez les conteneurs qui pourraient utiliser les mêmes ports de connexion.
   - Exécutez la commande suivante pour arrêter les conteneurs Docker et supprimer les volumes :
     ```bash
     docker-compose down --volumes
     ```

6. **Lancer les conteneurs Docker :**
   - Exécutez la commande suivante pour démarrer les conteneurs Docker :
     ```bash
     docker-compose up
     ```

7. **Accéder à l'interface Airflow :**
   - Une fois le conteneur lancé, accédez à [http://localhost:8081](http://localhost:8081) pour accéder à l'interface Airflow.
   - Connectez-vous en utilisant le nom d'utilisateur (airflow) et le mot de passe (airflow).

8. **Créer une connexion dans Airflow :**
   - Suivez les instructions fournies dans l'image 2 du fichier `support.pdf` pour créer une connexion dans Airflow.

9. **Activer et lancer le DAG HealthDataWarehousing :**
   - Activez le DAG `HealthDataWarehousing` dans l'interface Airflow.
   - Lancez le DAG et assurez-vous que les 3 tâches sont complètement exécutées (vertes).

10. **Ouvrir le notebook de visualisation :**
    - Ouvrez le notebook de visualisation et exécutez les cellules déjà remplies pour illustrer quelques résultats.

