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

#### Pour compiler :
```bash
cd code
make
```

#### Pour faire tourner :
##### `main` des codes séquentiels : 
```bash
./test
# ou
./demo
```

##### `main` des codes distribués avec MPI :
```bash
#mpirun -np nb_processeurs -host [noms des machines separes par ,] ./fichier/a/executer
mpirun -np 3 MPItest #on simule des machines distantes sur le host local
# ou
mpirun -np 3 -host allemagne,espagne,pologne MPItest #par exemple

#avec SLURM :
#salloc -n nb_processeurs --ntasks-per-node=nb_taches_par_proc mpirun ./fichier/a/executer
salloc -n 6 --ntasks-per-node=2 mpirun MPItest #par exemple
```

### Remarques

On a choisi de mettre la donnée de "quelles variables sont stockées dans quelles colonnes de la table" (list of variables) comme attribut `z` de la classe `Relation`.

Cela évite d'avoir à passer la liste des variables à chaque opération où on a besoin de savoir à quoi renvoient les colonnes, typiquement pour join...

L'inconvénient est que cela complique les opérations de join d'une table sur elle-même, car cela nécessite de connaître les données et les deux listes de variables. La solution naturelle est de faire une copie de la table, de changer la liste de variables de la copie, puis de join les deux tables (l'originale et la copie) ; mais cela suppose de copier toutes les entrées de la table... Alors que `join` renvoie lui-même une nouvelle Relation.

### Données de test

On a pris les données proposées par l'auteur du sujet à l'adresse indiquée dans le sujet :
  2099732 dblp.dat
   176468 facebook.dat
  4841532 twitter.dat
  7117732 total

### `MPIjoin.cpp` et `MPIjoin_copydata.cpp`

Ces deux fichiers sont deux implémentaions de task 5, l'une avec le paradigme MPI usuel où on considère que tous les processeurs ont accès aux données (partagées grâce à nfs ou quelque chose comme ça), l'autre où le root copie les données sur le réseau pour les envoyer aux processeurs (en n'envoyant que les données qui le concernent à chaque processeur, bien sûr).

`MPIjoin_copydata.cpp` est légèrement plus compliqué. En fait on n'avait pas réalisé, au moment de faire task 5, que copier les entrées sur le réseau n'était pas nécessaire... d'où cette première version.

### License

WTFPL... jk, **ISC**
