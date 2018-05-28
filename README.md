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

### License

WTFPL... jk, **ISC**
