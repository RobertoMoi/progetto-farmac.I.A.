# progetto-farmac.I.A.

Questo progetto utilizza PySpark per elaborare i dati dei pazienti da un file Excel e calcolare i tassi di aderenza in base alle date di consegna e ai giorni di terapia. Successivamente va ad esportare i dati in file Excel individuali per ciascun farmaco, così come un file consolidato contenente tutti i dati.

## Prerequisiti

- Un'istanza EC2 di Amazon con Ubuntu 22.04.
- Java 8 o superiore.
- Apache Spark.
- Python 3 e pip.
- PySpark
- Pandas
- openpyxl


## Esecuzione del progetto
1. Carica il file `progetto_farmacia_lastversion.py` sulla tua istanza EC2 attraverso il comando `scp -i /path/to/your-key-pair.pem /path/to/local/progetto_farmacia_lastversion.py ubuntu@your-ec2-instance-public-dns:/home/ubuntu/`
2. Carica i files `dati_pazienti.xlsx` e `dati_pazientiV3.xlsx` sulla tua istanza EC2 attraverso il comando `scp -i /path/to/your-key-pair.pem /path/to/local/dati_pazienti.xlsx /path/to/local/dati_pazientiV3.xlsx ubuntu@your-ec2-instance-public-dns:/home/ubuntu/`
3. Esegui il file con il comando `spark-submit progetto_farmacia_lastversion.py`.

## Output finale
Verranno generati i files xlsx (per ogni farmaco) con i dati dei pazienti e la media giorni di ogni ritiro, numero di consegne totali e il numero di ritardi. Gli ultimi files generati saranno quelli relativi all'aderenza (sia per ogni farmaco che uno totale).
