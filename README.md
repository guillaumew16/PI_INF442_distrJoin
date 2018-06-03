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

#### list of variables

On a choisi de mettre la donnée de "quelles variables sont stockées dans quelles colonnes de la table" (list of variables) comme attribut `z` de la classe `Relation`.

Cela évite d'avoir à passer la liste des variables à chaque opération où on a besoin de savoir à quoi renvoient les colonnes, typiquement pour join...

L'inconvénient est que cela complique les opérations de join d'une table sur elle-même, car cela nécessite de connaître les données et les deux listes de variables. La solution naturelle est de faire une copie de la table, de changer la liste de variables de la copie, puis de join les deux tables (l'originale et la copie) ; mais cela suppose de copier toutes les entrées de la table... Alors que `join` renvoie lui-même une nouvelle Relation. 

Une autre solution serait d'adapter le code de `join` pour réécrire totalement `autoJoin`, pour diviser par deux l'espace mémoire requis en représentant la "copie" par des permutations sur les entrées. Mais c'est compliqué pour un gain faible, d'autant plus que pour presque toutes nos applications, les arités sont de 2 ou 3.

### Données de test

On a pris les données proposées par l'auteur du sujet à l'adresse indiquée dans le sujet (copiées dans le dossier `data/`:
  2099732 dblp.dat
   176468 facebook.dat
  4841532 twitter.dat
  7117732 total

Pour tester la correction on a également utilisé des troncatures de ces fichiers à 1000 lignes (`data_head/`).

### `MPIjoin_nfs.cpp` et `MPIjoin_copydata.cpp`

Ces deux fichiers sont deux implémentaions de task 5, l'une avec le paradigme MPI usuel où on considère que tous les processeurs ont accès aux données (partagées grâce à nfs ou quelque chose comme ça), l'autre où le root copie les données sur le réseau pour les envoyer aux processeurs (en n'envoyant que les données qui le concernent à chaque processeur, bien sûr).

`MPIjoin_copydata.cpp` passe sur le réseau les données input comme les données output. L'idée est que le processeur root envoie chaque entrée au processeur concerné, puis une fois qu'on est sûr que les données ont toutes été reçues, on envoie un signal "fin des données input" aux processeurs (grâce aux "tags" MPI). Puis chaque processeur effectue le join sur ce qu'il a reçu et renvoie le résultat à root
On n'avait pas réalisé, au moment de faire task 5, que copier les inputs sur le réseau n'était pas nécessaire, d'où cette première version.

`MPIjoin_nfs.cpp` s'appuie donc sur l'hypothèse que les Relations données en arguments sont accessibles par tous les processeurs, donc on n'envoie sur le réseau que les données à output.

### *Task 7*

On n'a pas traité *Task 7*, car c'est la seule tâche pour laquelle il est nécessaire d'envoyer des messages entre processeurs non-root. Or, pour faire cela, `MPIjoin_nfs` ne convient pas, puisque les données input ne sont jamais passées explicitement sur le réseau ; et `MPIjoin_copydata` non plus, car chaque processeur devrait alors à la fois envoyer et recevoir des signaux "fin des données input", ce qui pose un problème de deadlock.

On a pensé à faire une troisième version, où au lieu d'envoyer des signaux de "fin de données", on envoie à l'avance le nombre de données qui vont être envoyées. Cependant nous n'avons pas assez de temps. D'ailleurs cette troisième version serait plus sensible à la qualité du réseau, car si un des messages ne parvient pas à destination, le processeur destinataire sera bloqué, contrairement à la deuxième version (`MPIjoin_copydata`) où le programme terminera quand même, avec quelques données manquantes dans l'output.

### License

WTFPL... jk, **ISC**
