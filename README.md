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

### Données de test

On a pris les données proposées par l'auteur du sujet à l'adresse indiquée dans le sujet (copiées dans le dossier `data/`:
  2099732 dblp.dat
   176468 facebook.dat
  4841532 twitter.dat
  7117732 total

Pour tester la correction on a également utilisé des troncatures de ces fichiers à 1000 lignes (`data_head/`).

### `MPIjoin_nfs.cpp` et `MPIjoin_copydata.cpp`

Ces deux fichiers sont deux implémentaions de task 5 à 8, l'une avec le paradigme MPI usuel où on considère que tous les processeurs ont accès aux données (partagées grâce à nfs ou quelque chose comme ça), l'autre où le root copie les données sur le réseau pour les envoyer aux processeurs (en n'envoyant que les données qui le concernent à chaque processeur, bien sûr).

`MPIjoin_copydata.cpp` passe sur le réseau les données input comme les données output. L'idée est que le processeur root envoie chaque entry au processeur concerné, puis une fois qu'on est sûr que les données ont toutes été reçues, on envoie un signal "fin des données input" aux processeurs (grâce aux "tags" MPI). Puis chaque processeur effectue le join sur ce qu'il a reçu et renvoie le résultat à root
On n'avait pas réalisé, au moment de faire task 5, que copier les inputs sur le réseau n'était pas nécessaire, d'où cette première version.

`MPIjoin_nfs.cpp` s'appuie donc sur l'hypothèse que les Relations données en arguments sont accessibles par tous les processeurs, donc on n'envoie sur le réseau que les données à output.

Dans le code fourni, `MPIjoin_copydata` et `MPIjoin_nfs` répondent tous deux aux tâches 5, 6 et 8. La tâche 7 n'est pas réalisée (cf infra). La tâche 8 n'est réalisée que partiellement, on n'a que hyperCubeTriangle (hyperCubeMultiJoin n'est pas implémenté dans l'un et bugué dans l'autre).

Pour changer de méthode, décommenter/recommenter les lignes correspondant à `MPIjoin.o` dans le Makefile. (On a fait le choix de garder le Makefile aussi simple et lisible que possible.)

### *Task 7*

On n'a pas traité *Task 7*, car c'est la seule tâche pour laquelle il est nécessaire d'envoyer des messages entre processeurs non-root. Or, pour faire cela, `MPIjoin_nfs` ne convient pas, puisque les données input ne sont jamais passées explicitement sur le réseau ; et `MPIjoin_copydata` non plus, car chaque processeur devrait alors à la fois envoyer et recevoir des signaux "fin des données input", ce qui pose un problème de deadlock.

On a pensé à faire une troisième version, où au lieu d'envoyer des signaux de "fin de données", on envoie à l'avance le nombre de données qui vont être envoyées. Cependant nous n'avons pas assez de temps. D'ailleurs cette troisième version serait plus sensible à la qualité du réseau, car si un des messages ne parvient pas à destination, le processeur destinataire sera bloqué, contrairement à la deuxième version (`MPIjoin_copydata`) où le programme terminera quand même, avec quelques données manquantes dans l'output.

### Remarques

#### list of variables - stockage

On a choisi de mettre la donnée de "quelles variables sont stockées dans quelles colonnes de la table" (list of variables) comme attribut `z` de la classe `Relation`.

Cela évite d'avoir à passer la liste des variables à chaque opération où on a besoin de savoir à quoi renvoient les colonnes, typiquement pour join...

L'inconvénient est que cela complique les opérations de join d'une table sur elle-même, car cela nécessite de connaître les données et les deux listes de variables. La solution naturelle est de faire une copie de la table, de changer la liste de variables de la copie, puis de join les deux tables (l'originale et la copie) ; mais cela suppose de copier toutes les entries de la table... Alors que `join` renvoie lui-même une nouvelle Relation. 

Une autre solution serait d'adapter le code de `join` pour réécrire totalement `autoJoin`, pour diviser par deux l'espace mémoire requis en représentant la "copie" par des permutations sur les entries. Mais c'est compliqué pour un gain faible, d'autant plus que pour presque toutes nos applications, les arités sont de 2 ou 3.

#### list of variables - nature

On n'autorise pas la répétition de variables dans une liste de variables. Par exemple, ceci est illégal : 
`rel.getVariables() = [0, 1, 1]`.
 En effet, si `rel` contient une entrée 
`rel.getEntry(xxx) = [10, 20, 30]`,
cette entrée signifie que 
`x0=10, x1=20, x1=20`. 
Or dans une même valuation ("assignment" dans le sujet) une variable ne peut pas avoir deux valeurs différentes !

En revanche les variables n'ont pas besoin d'être consécutives. On peut tout à fait avoir 
`rel.getVariables() = [23, 5, 111]`.
Cela signife simplement que l'entrée
`rel.getEntry(xxx) = [10, 20, 30]`,
correspond à un "assignment"
`x23=10, x5=20, x111=30`. 
Cela ne pose pas de problème. (D'ailleurs on utilise des variables non-consécutives quand on calcule les triangles : on utilise la liste de variables `[0, 2]`.)

#### debugage avec MPI

Petite heuristique : quand on débugue un programme qui termine en renvoyant une mauvaise réponse, un bon moyen de savoir si le problème vient de MPI est de l'exécuter plusieurs fois sur les mêmes input. Si cela donne des réponses différentes (typiquement, des nombres d'entries différents) c'est qu'il y a probablement un problème dans l'ordre de passage des messages.

> https://stackoverflow.com/questions/329259/how-do-i-debug-an-mpi-program
>
> I have found gdb quite useful. I use it as
> ```bash
> mpirun -np <NP> xterm -e gdb ./program 
> ```
> This the launches xterm windows in which I can do
> ```bash
> run <arg1> <arg2> ... <argN>
> ```
> Usually works fine.
> You can also package these commands together using:
> ```bash
> mpirun -n <NP> xterm -hold -e gdb -ex run --args ./program [arg1] [arg2] [...]
> ```

### Remarques de la soutenance

Remarques du correcteur. Pas pris en compte dans le code du coup.

- Convertir des vectors en arrays C++ natifs n'est pas évident, la syntaxe `&myVector[0]` utilisable en C++ n'est pas forcément bien interprétée par MPI. (En l'occurrence ça semblait marcher dans les tests effectués.)
- Lorsqu'on charge des données dans la RAM depuis le disque dur, il faut faire attention à si on le charge dans la pile ou dans le tas.
  - Pile : taille ~ 1 Mo ; objets explicites (pas de `new`).
  - Tas : taille ~ 2 Go par processus (total=le reste de la RAM) ; pointeurs et `new`.
- Pour les deux raisons précédentes, la bonne façon de représenter des tableaux pour MPI est probablement encore des pointeurs, en stockant les dimensions dans des champs séparés de la classe. Par exemple,
```C++
class Relation {
public:
  /*...*/
private:
  int r;
  int size;
  unsigned int **entries;
  int *z;
}
```
- Entre charger les données input seulement pour le root et les envoyer par MPI, et les faire charger par tous les processeurs, les deux se font.
- MPI boost permet de sérialiser des données pour les envoyer plus facilement avec MPI.

### License

**ISC**
