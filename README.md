PI INF442: Distributed Join Processing on Social Network Data
===

Authors: Guillaume WANG & Pierre-Louis NORDMANN

### Structure du dossier
- `code/`: nos solutions.
    - `hash_function/`: code source de la fonction de hachage MurmurHash3 telle que téléchargée depuis https://github.com/aappleby/smhasher/
- `Conteneurs/`: un test de Pierre-Louis pour regarder à quel point les choix de structures jouent sur les temps d'exécution
- `data/` (git ignored): les données mises à disposition par le sujet. Git ignored car c'est lourd et inutile à tracker
- `data_head/`: fichiers .dat de test rapide. Inclut des input entrées à la main et des données issues de `data/` mais tronquées
- `output/` (git ignored): les résultats de tests. Git ignored car les résultats de tests sur des inputs de `data/` peuvent être lourds
- `.gitignore`
- `distrJoin.pdf`
- `README.md`

### Utilisation

```bash
cd code
make
./test
```

### Remarques

On a choisi de mettre la donnée de "quelles variables sont stockées dans quelles colonnes de la table" (list of variables) comme attribut `z` de la classe `Relation`.

Cela évite d'avoir à passer la liste des variables à chaque opération où on a besoin de savoir à quoi renvoient les colonnes, typiquement pour join...

L'inconvénient est que cela complique les opérations de join d'une table sur elle-même, car cela nécessite de connaître les données et les deux listes de variables. La solution naturelle est de faire une copie de la table, de changer la liste de variables de la copie, puis de join les deux tables (l'originale et la copie) ; mais cela suppose de copier toutes les entrées de la table... Alors que `join` renvoie lui-même une nouvelle Relation.

### License

WTFPL... jk, **ISC**
