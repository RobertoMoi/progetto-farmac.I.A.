
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd

sc = SparkContext()

# Funzione per calcolare la differenza in giorni tra date
def calculate_days_between_deliveries(index, iterator):
    prev_record = None
    result = []
    num_consegne = 0

    for record in iterator:
      current_date = record['DT_EROG']

      if prev_record and record['CODICE PAZIENTE UNIVOCO'] == prev_record['CODICE PAZIENTE UNIVOCO']:  # Stesso paziente
        num_consegne += 1
        prev_date = prev_record['DT_EROG']
        days_diff = abs((current_date - prev_date).days)  # Differenza in giorni, sempre positiva
        result.append(({'codice paziente univoco': prev_record['CODICE PAZIENTE UNIVOCO'],
                        'data consegna':prev_record['DT_EROG'],
                        'data consegna successiva': record['DT_EROG'],
                        'quantità consegnata': prev_record['QTA'],
                        'giorni passati dall\'ultima consegna': days_diff,
                        'numero di ritiri': num_consegne}))
      else:
        num_consegne = 1
        result.append(({'codice paziente univoco': record['CODICE PAZIENTE UNIVOCO'],
                        'data consegna':record['DT_EROG'],
                        'data consegna successiva': None,
                        'quantità consegnata': record['QTA'],
                        'giorni passati dall\'ultima consegna': 0,
                        'numero di ritiri': num_consegne}))


      prev_record = record

    return iter(result)

def reduce_dates(x, y):
    # x e y sono tuple (max_date, min_date)
    return (max(x[0], y), min(x[1], y))

# creazione di una nuova sessione spark
spark = SparkSession.builder.getOrCreate()

"""### Lettura del file excel e conversione del dataframe in rdd"""

# lettura file excel con dati pazienti
df = pd.read_excel('dati_pazienti.xlsx')

# conversione delle date di ritiro nel formato datetime
df['DT_EROG'] = pd.to_datetime(df['DT_EROG'], dayfirst=True)

# conversione dataframe pandas in dataframe spark
df_spark = spark.createDataFrame(df)

# conversione DataFrame di Spark in un RDD
rdd = df_spark.rdd

# split del dataset in base al codice del farmaco
drug_code = [1480022, 1470395, 1480002, 1487505]
# ogni rdd contiene i dati dei pazienti del farmaco corrispondente
rdd_aimovig70 = rdd.filter(lambda x: x['PROD'] == drug_code[0]).map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']), (x['SESSO'], x['DT_NAS'], x['COMUNE NASCITA'], x['COMUNE_RESIDENZA'], x['ASL_APPARTENENZA']))).distinct()
rdd_aimovig140 = rdd.filter(lambda x: x['PROD'] == drug_code[1]).map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']), (x['SESSO'], x['DT_NAS'], x['COMUNE NASCITA'], x['COMUNE_RESIDENZA'], x['ASL_APPARTENENZA']))).distinct()
rdd_emgality = rdd.filter(lambda x: x['PROD'] == drug_code[2]).map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']), (x['SESSO'], x['DT_NAS'], x['COMUNE NASCITA'], x['COMUNE_RESIDENZA'], x['ASL_APPARTENENZA']))).distinct()
rdd_ajovy = rdd.filter(lambda x: x['PROD'] == drug_code[3]).map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']), (x['SESSO'], x['DT_NAS'], x['COMUNE NASCITA'], x['COMUNE_RESIDENZA'], x['ASL_APPARTENENZA']))).distinct()

# rdd filtrati in base ai farmaci con tutti i dati del dataset originale
rdd_aimovig70_filtered = rdd.filter(lambda x: x['PROD'] == drug_code[0])
rdd_aimovig140_filtered = rdd.filter(lambda x: x['PROD'] == drug_code[1])
rdd_emgality_filtered = rdd.filter(lambda x: x['PROD'] == drug_code[2])
rdd_ajovy_filtered = rdd.filter(lambda x: x['PROD'] == drug_code[3])

# ordina gli rdd in base al codice paziente univoco e sucessivamente al giorno dell'anno
rdd_aimovig70_sorted = rdd_aimovig70_filtered.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG']))
rdd_aimovig140_sorted = rdd_aimovig140_filtered.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG']))
rdd_emgality_sorted = rdd_emgality_filtered.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG']))
rdd_ajovy_sorted = rdd_ajovy_filtered.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG']))

# estrazione prima e ultima data di consegna di un farmaco
all_rdd_sorted = [rdd_aimovig70_sorted, rdd_aimovig140_sorted, rdd_emgality_sorted, rdd_ajovy_sorted]

for actual_rdd in all_rdd_sorted:
  rdd_first_last_date = actual_rdd.map(lambda x: (x['CODICE PAZIENTE UNIVOCO'],(x['DT_EROG'])))

  rdd_first_date = rdd_first_last_date.reduceByKey(lambda x,y: (min(x,y)))
  rdd_last_date = rdd_first_last_date.reduceByKey(lambda x,y: (max(x,y)))
  rdd_dates = rdd_first_date.join(rdd_last_date)

  if actual_rdd == rdd_aimovig70_sorted:
    rdd_aimovig70_dates = rdd_dates.map(lambda x:  ((x[0]),(x[1][0],x[1][1])))
  elif actual_rdd == rdd_aimovig140_sorted:
    rdd_aimovig140_dates = rdd_dates.map(lambda x:  ((x[0]),(x[1][0],x[1][1])))
  elif actual_rdd == rdd_emgality_sorted:
    rdd_emgality_dates = rdd_dates.map(lambda x:  ((x[0]),(x[1][0],x[1][1])))
  else:
    rdd_ajovy_dates = rdd_dates.map(lambda x:  ((x[0]),(x[1][0],x[1][1])))

rdd_aimovig70_dates.collect()

# nuovo rdd con il codice paziente, data di consegna, data di consegna successiva, quantità consegnata e giorni passati tra due consegne
rdd_aimovig70_with_delivery_info = rdd_aimovig70_sorted.mapPartitionsWithIndex(calculate_days_between_deliveries)
rdd_aimovig140_with_delivery_info = rdd_aimovig140_sorted.mapPartitionsWithIndex(calculate_days_between_deliveries)
rdd_emgality_with_delivery_info = rdd_emgality_sorted.mapPartitionsWithIndex(calculate_days_between_deliveries)
rdd_ajovy_with_delivery_info = rdd_ajovy_sorted.mapPartitionsWithIndex(calculate_days_between_deliveries)

# check rdd con informazioni riguardo le consegne
for record in rdd_aimovig70_with_delivery_info.collect():
    print(record)

# normalizzazione dei giorni passati dall'ultima consegna (in caso la quantità consegnata fosse più di una) e aggiunta di un contatore per il calcolo della media dei giorni
rdd_aimovig70_pre_average_days = rdd_aimovig70_with_delivery_info.map(lambda x : (x['codice paziente univoco'], (x['giorni passati dall\'ultima consegna']/ x['quantità consegnata'], 1)))
rdd_aimovig140_pre_average_days = rdd_aimovig140_with_delivery_info.map(lambda x : (x['codice paziente univoco'], (x['giorni passati dall\'ultima consegna']/ x['quantità consegnata'], 1)))
rdd_emgality_pre_average_days = rdd_emgality_with_delivery_info.map(lambda x : (x['codice paziente univoco'], (x['giorni passati dall\'ultima consegna']/ x['quantità consegnata'], 1)))
rdd_ajovy_pre_average_days = rdd_ajovy_with_delivery_info.map(lambda x : (x['codice paziente univoco'], (x['giorni passati dall\'ultima consegna']/ x['quantità consegnata'], 1)))

# calcolo dei giorni mediamente passati ad ogni consegna
rdd_aimovig70_average_days = rdd_aimovig70_pre_average_days.reduceByKey(lambda x,y: ((x[0]+y[0]), x[1]+y[1])).map(lambda x: (x[0], round(x[1][0]/x[1][1], 0)))
rdd_aimovig140_average_days = rdd_aimovig140_pre_average_days.reduceByKey(lambda x,y: ((x[0]+y[0]), x[1]+y[1])).map(lambda x: (x[0], round(x[1][0]/x[1][1], 0)))
rdd_emgality_average_days = rdd_emgality_pre_average_days.reduceByKey(lambda x,y: ((x[0]+y[0]), x[1]+y[1])).map(lambda x: (x[0], round(x[1][0]/x[1][1], 0)))
rdd_ajovy_average_days = rdd_ajovy_pre_average_days.reduceByKey(lambda x,y: ((x[0]+y[0]), x[1]+y[1])).map(lambda x: (x[0], round(x[1][0]/x[1][1], 0)))

# calcolo dei giorni in cui il paziente ha ritirato il farmaco in ritardo
rdd_aimovig70_num_days_delay = rdd_aimovig70_pre_average_days.join(rdd_aimovig70_average_days).map(lambda x: (x[0], (x[1][0][0], x[1][1], 1 if x[1][0][0] > x[1][1]+3 else 0))).map(lambda x: (x[0], (x[1][2]))).reduceByKey(lambda x,y: x+y)
rdd_aimovig140_num_days_delay = rdd_aimovig140_pre_average_days.join(rdd_aimovig140_average_days).map(lambda x: (x[0], (x[1][0][0], x[1][1], 1 if x[1][0][0] > x[1][1]+3 else 0))).map(lambda x: (x[0], (x[1][2]))).reduceByKey(lambda x,y: x+y)
rdd_emgality_num_days_delay = rdd_emgality_pre_average_days.join(rdd_emgality_average_days).map(lambda x: (x[0], (x[1][0][0], x[1][1], 1 if x[1][0][0] > x[1][1]+3 else 0))).map(lambda x: (x[0], (x[1][2]))).reduceByKey(lambda x,y: x+y)
rdd_ajovy_num_days_delay = rdd_ajovy_pre_average_days.join(rdd_ajovy_average_days).map(lambda x: (x[0], (x[1][0][0], x[1][1], 1 if x[1][0][0] > x[1][1]+3 else 0))).map(lambda x: (x[0], (x[1][2]))).reduceByKey(lambda x,y: x+y)

# creazione di un rdd con il numero totale delle consegne effettuate ad un paziente
rdd_aimovig70_with_number_of_deliveries =  rdd_aimovig70_with_delivery_info.map(lambda x : (x['codice paziente univoco'],(x['numero di ritiri']))).reduceByKey(lambda x,y: max(x,y))
rdd_aimovig140_with_number_of_deliveries =  rdd_aimovig140_with_delivery_info.map(lambda x : (x['codice paziente univoco'],(x['numero di ritiri']))).reduceByKey(lambda x,y: max(x,y))
rdd_emgality_with_number_of_deliveries =  rdd_emgality_with_delivery_info.map(lambda x : (x['codice paziente univoco'],(x['numero di ritiri']))).reduceByKey(lambda x,y: max(x,y))
rdd_ajovy_with_number_of_deliveries =  rdd_ajovy_with_delivery_info.map(lambda x : (x['codice paziente univoco'],(x['numero di ritiri']))).reduceByKey(lambda x,y: max(x,y))

# unione degli rdd con il numero di giorni ritardo e media giorni
rdd_aimovig70_average_days_and_delay = rdd_aimovig70_num_days_delay.join(rdd_aimovig70_average_days)
rdd_aimovig140_average_days_and_delay = rdd_aimovig140_num_days_delay.join(rdd_aimovig140_average_days)
rdd_emgality_average_days_and_delay = rdd_emgality_num_days_delay.join(rdd_emgality_average_days)
rdd_ajovy_average_days_and_delay = rdd_ajovy_num_days_delay.join(rdd_ajovy_average_days)

# unione degli rdd con il numero di giorni ritardo, numero di consegne del farmaco e media giorni
rdd_aimovig70_with_all_measures = rdd_aimovig70_average_days_and_delay.join(rdd_aimovig70_with_number_of_deliveries).map(lambda x: ((x[0]), (x[1][0][0], int(x[1][0][1]), x[1][1])))
rdd_aimovig140_with_all_measures = rdd_aimovig140_average_days_and_delay.join(rdd_aimovig140_with_number_of_deliveries).map(lambda x: ((x[0]), (x[1][0][0], int(x[1][0][1]), x[1][1])))
rdd_emgality_with_all_measures = rdd_emgality_average_days_and_delay.join(rdd_emgality_with_number_of_deliveries).map(lambda x: ((x[0]), (x[1][0][0], int(x[1][0][1]), x[1][1])))
rdd_ajovy_with_all_measures = rdd_ajovy_average_days_and_delay.join(rdd_ajovy_with_number_of_deliveries).map(lambda x: ((x[0]), (x[1][0][0], int(x[1][0][1]), x[1][1])))

# creazione rdd aimovig70 con tutte le informazioni
rdd_aimovig70_with_all_info = rdd_aimovig70.join(rdd_aimovig70_with_all_measures)
rdd_aimovig70_with_all_info = rdd_aimovig70_with_all_info.join(rdd_aimovig70_dates)
rdd_aimovig70_with_all_info = rdd_aimovig70_with_all_info.reduceByKey(lambda x,y: x)
rdd_aimovig70_with_all_info = rdd_aimovig70_with_all_info.map(lambda x: {
                                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                                        'SESSO': x[1][0][0][0],
                                                                        'DT_NAS': x[1][0][0][1],
                                                                        'COMUNE NASCITA': x[1][0][0][2],
                                                                        'COMUNE_RESIDENZA': x[1][0][0][3],
                                                                        'ASL_APPARTENENZA': x[1][0][0][4],
                                                                        'DATA PRIMA CONSEGNA': x[1][1][0],
                                                                        'DATA ULTIMA CONSEGNA': x[1][1][1],
                                                                        'MEDIA GIORNI': x[1][0][1][1],
                                                                        'NUMERO CONSEGNE TOTALI': x[1][0][1][2],
                                                                        'NUMERO RITARDI': x[1][0][1][0],
                                                                        })

# creazione rdd aimovig140 con tutte le informazioni
rdd_aimovig140_with_all_info = rdd_aimovig140.join(rdd_aimovig140_with_all_measures)
rdd_aimovig140_with_all_info = rdd_aimovig140_with_all_info.join(rdd_aimovig140_dates)
rdd_aimovig140_with_all_info = rdd_aimovig140_with_all_info.reduceByKey(lambda x,y: x)
rdd_aimovig140_with_all_info = rdd_aimovig140_with_all_info.map(lambda x: {
                                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                                        'SESSO': x[1][0][0][0],
                                                                        'DT_NAS': x[1][0][0][1],
                                                                        'COMUNE NASCITA': x[1][0][0][2],
                                                                        'COMUNE_RESIDENZA': x[1][0][0][3],
                                                                        'ASL_APPARTENENZA': x[1][0][0][4],
                                                                        'DATA PRIMA CONSEGNA': x[1][1][0],
                                                                        'DATA ULTIMA CONSEGNA': x[1][1][1],
                                                                        'MEDIA GIORNI': x[1][0][1][1],
                                                                        'NUMERO CONSEGNE TOTALI': x[1][0][1][2],
                                                                        'NUMERO RITARDI': x[1][0][1][0],
                                                                        })

# creazione rdd emgality con tutte le informazioni
rdd_emgality_with_all_info = rdd_emgality.join(rdd_emgality_with_all_measures)
rdd_emgality_with_all_info = rdd_emgality_with_all_info.join(rdd_emgality_dates)
rdd_emgality_with_all_info = rdd_emgality_with_all_info.reduceByKey(lambda x,y: x)
rdd_emgality_with_all_info = rdd_emgality_with_all_info.map(lambda x: {
                                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                                        'SESSO': x[1][0][0][0],
                                                                        'DT_NAS': x[1][0][0][1],
                                                                        'COMUNE NASCITA': x[1][0][0][2],
                                                                        'COMUNE_RESIDENZA': x[1][0][0][3],
                                                                        'ASL_APPARTENENZA': x[1][0][0][4],
                                                                        'DATA PRIMA CONSEGNA': x[1][1][0],
                                                                        'DATA ULTIMA CONSEGNA': x[1][1][1],
                                                                        'MEDIA GIORNI': x[1][0][1][1],
                                                                        'NUMERO CONSEGNE TOTALI': x[1][0][1][2],
                                                                        'NUMERO RITARDI': x[1][0][1][0],
                                                                        })

# creazione rdd ajovy con tutte le informazioni
rdd_ajovy_with_all_info = rdd_ajovy.join(rdd_ajovy_with_all_measures)
rdd_ajovy_with_all_info = rdd_ajovy_with_all_info.join(rdd_ajovy_dates)
rdd_ajovy_with_all_info = rdd_ajovy_with_all_info.reduceByKey(lambda x,y: x)
rdd_ajovy_with_all_info = rdd_ajovy_with_all_info.map(lambda x: {
                                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                                        'SESSO': x[1][0][0][0],
                                                                        'DT_NAS': x[1][0][0][1],
                                                                        'COMUNE NASCITA': x[1][0][0][2],
                                                                        'COMUNE_RESIDENZA': x[1][0][0][3],
                                                                        'ASL_APPARTENENZA': x[1][0][0][4],
                                                                        'DATA PRIMA CONSEGNA': x[1][1][0],
                                                                        'DATA ULTIMA CONSEGNA': x[1][1][1],
                                                                        'MEDIA GIORNI': x[1][0][1][1],
                                                                        'NUMERO CONSEGNE TOTALI': x[1][0][1][2],
                                                                        'NUMERO RITARDI': x[1][0][1][0],
                                                                        })

# inserimento nei dataframe di tutte le informazioni raccolte
df_aimovig70 = pd.DataFrame(rdd_aimovig70_with_all_info.collect())
df_aimovig140 = pd.DataFrame(rdd_aimovig140_with_all_info.collect())
df_emgality120 = pd.DataFrame(rdd_emgality_with_all_info.collect())
df_ajovy = pd.DataFrame(rdd_ajovy_with_all_info.collect())

# cambio del formato data
df_aimovig70['DATA PRIMA CONSEGNA'] = df_aimovig70['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_aimovig70['DATA ULTIMA CONSEGNA'] = df_aimovig70['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_aimovig140['DATA PRIMA CONSEGNA'] = df_aimovig140['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_aimovig140['DATA ULTIMA CONSEGNA'] = df_aimovig140['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_emgality120['DATA PRIMA CONSEGNA'] = df_emgality120['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_emgality120['DATA ULTIMA CONSEGNA'] = df_emgality120['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_ajovy['DATA PRIMA CONSEGNA'] = df_ajovy['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_ajovy['DATA ULTIMA CONSEGNA'] = df_ajovy['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

# nomi dei file xlsx finali
names_file_excel = ["dati_aimovig70_046925018.xlsx", "dati_aimovig140_046925044.xlsx", "dati_emgality120_047424015.xlsx", "dati_ajovy_047791013.xlsx"]

# conversione dataframe in file xlsx
df_aimovig70.to_excel(names_file_excel[0], index=False)
df_aimovig140.to_excel(names_file_excel[1], index=False)
df_emgality120.to_excel(names_file_excel[2], index=False)
df_ajovy.to_excel(names_file_excel[3], index=False)

# lettura file excel con dati pazienti
df = pd.read_excel('dati_pazientiV3.xlsx')

# conversione delle date di ritiro nel formato datetime
df['DT_EROG'] = pd.to_datetime(df['DT_EROG'], dayfirst=True)

# conversione dataframe pandas in dataframe spark
df_spark = spark.createDataFrame(df)

# conversione DataFrame di Spark in un RDD
rdd_v2 = df_spark.rdd

# rdd filtrati in base ai farmaci con tutti i dati del dataset originale
rdd_aimovig70_filtered2 = rdd_v2.filter(lambda x: x['PROD'] == drug_code[0])
rdd_aimovig140_filtered2 = rdd_v2.filter(lambda x: x['PROD'] == drug_code[1])
rdd_emgality_filtered2 = rdd_v2.filter(lambda x: x['PROD'] == drug_code[2])
rdd_ajovy_filtered2 = rdd_v2.filter(lambda x: x['PROD'] == drug_code[3])

# rdd filtrati in base ai farmaci con tutti i dati del dataset originale
rdd_aimovig70_date_therapydays = rdd_aimovig70_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))
rdd_aimovig140_date_therapydays = rdd_aimovig140_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))
rdd_emgality_date_therapydays = rdd_emgality_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))
rdd_ajovy_date_therapydays = rdd_ajovy_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))

rdd_aimovig70_date_therapydays_red = rdd_aimovig70_date_therapydays.reduceByKey(lambda x,y: x if x[0] >= y[0] else y).map(lambda x: ((x[0]), x[1][1]))
rdd_aimovig140_date_therapydays_red = rdd_aimovig140_date_therapydays.reduceByKey(lambda x,y: x if x[0] >= y[0] else y).map(lambda x: ((x[0]), x[1][1]))
rdd_emgality_date_therapydays_red = rdd_emgality_date_therapydays.reduceByKey(lambda x,y: x if x[0] >= y[0] else y).map(lambda x: ((x[0]), x[1][1]))
rdd_ajovy_date_therapydays_red = rdd_ajovy_date_therapydays.reduceByKey(lambda x,y: x if x[0] >= y[0] else y).map(lambda x: ((x[0]), x[1][1]))

# ordina gli rdd in base al codice paziente univoco e sucessivamente al giorno dell'anno
rdd_aimovig70_sorted_with_dates = rdd_aimovig70_filtered2.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG'])).map(lambda x: (x['CODICE PAZIENTE UNIVOCO'],(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))
rdd_aimovig140_sorted_with_dates = rdd_aimovig140_filtered2.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG'])).map(lambda x: (x['CODICE PAZIENTE UNIVOCO'],(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))
rdd_emgality_sorted_with_dates = rdd_emgality_filtered2.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG'])).map(lambda x: (x['CODICE PAZIENTE UNIVOCO'],(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))
rdd_ajovy_sorted_with_dates = rdd_ajovy_filtered2.sortBy(lambda x: (x['CODICE PAZIENTE UNIVOCO'], x['DT_EROG'])).map(lambda x: (x['CODICE PAZIENTE UNIVOCO'],(x['DT_EROG'], x['giorni di terapia reali (PDD)'])))

# giorni di terapia totali per ogni paziente
rdd_therapydays_aimovig70 = rdd_aimovig70_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['giorni di terapia reali (PDD)']))).reduceByKey(lambda x,y: x+y)
rdd_therapydays_aimovig140 = rdd_aimovig140_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['giorni di terapia reali (PDD)']))).reduceByKey(lambda x,y: x+y)
rdd_therapydays_emgality = rdd_emgality_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['giorni di terapia reali (PDD)']))).reduceByKey(lambda x,y: x+y)
rdd_therapydays_ajovy = rdd_ajovy_filtered2.map(lambda x: ((x['CODICE PAZIENTE UNIVOCO']),(x['giorni di terapia reali (PDD)']))).reduceByKey(lambda x,y: x+y)

# separazione della chiave "CODICE PAZIENTE UNIVOCO" dagli altri dati per effettuare la join con gli rdd che contengono i giorni di terapia
rdd_aimovig70_with_key = rdd_aimovig70_with_all_info.map(lambda x: ({'CODICE PAZIENTE UNIVOCO': x['CODICE PAZIENTE UNIVOCO']}, {k: v for k, v in x.items() if k != 'CODICE PAZIENTE UNIVOCO'}))
rdd_aimovig70_with_key = rdd_aimovig70_with_key.map(lambda x: (x[0]['CODICE PAZIENTE UNIVOCO'], x[1]))

rdd_aimovig140_with_key = rdd_aimovig140_with_all_info.map(lambda x: ({'CODICE PAZIENTE UNIVOCO': x['CODICE PAZIENTE UNIVOCO']}, {k: v for k, v in x.items() if k != 'CODICE PAZIENTE UNIVOCO'}))
rdd_aimovig140_with_key = rdd_aimovig140_with_key.map(lambda x: (x[0]['CODICE PAZIENTE UNIVOCO'], x[1]))

rdd_emgality_with_key = rdd_emgality_with_all_info.map(lambda x: ({'CODICE PAZIENTE UNIVOCO': x['CODICE PAZIENTE UNIVOCO']}, {k: v for k, v in x.items() if k != 'CODICE PAZIENTE UNIVOCO'}))
rdd_emgality_with_key = rdd_emgality_with_key.map(lambda x: (x[0]['CODICE PAZIENTE UNIVOCO'], x[1]))

rdd_ajovy_with_key = rdd_ajovy_with_all_info.map(lambda x: ({'CODICE PAZIENTE UNIVOCO': x['CODICE PAZIENTE UNIVOCO']}, {k: v for k, v in x.items() if k != 'CODICE PAZIENTE UNIVOCO'}))
rdd_ajovy_with_key = rdd_ajovy_with_key.map(lambda x: (x[0]['CODICE PAZIENTE UNIVOCO'], x[1]))

# join tra l'rdd che contiene tutti i dati del paziente e l'rdd con i giorni di terapia
rdd_join_aimovig70 = rdd_aimovig70_with_key.join(rdd_therapydays_aimovig70)
rdd_join_aimovig140 = rdd_aimovig140_with_key.join(rdd_therapydays_aimovig140)
rdd_join_emgality = rdd_emgality_with_key.join(rdd_therapydays_emgality)
rdd_join_ajovy = rdd_ajovy_with_key.join(rdd_therapydays_ajovy)

rdd_join_aimovig70 = rdd_join_aimovig70.join(rdd_aimovig70_date_therapydays_red)
rdd_join_aimovig140 = rdd_join_aimovig140.join(rdd_aimovig140_date_therapydays_red)
rdd_join_emgality = rdd_join_emgality.join(rdd_emgality_date_therapydays_red)
rdd_join_ajovy = rdd_join_ajovy.join(rdd_ajovy_date_therapydays_red)

# calcolo dell'aderenza e correzione della formattazione
final_rdd_aimovig70 = rdd_join_aimovig70.map(lambda x: {
                                                        'PROD': '1480022',
                                                        'PROD_DESC': 'AIMOVIG*1PEN 70MG 1ML',
                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                        'SESSO': x[1][0][0]['SESSO'],
                                                        'DT_NAS': x[1][0][0]['DT_NAS'],
                                                        'COMUNE NASCITA': x[1][0][0]['COMUNE NASCITA'],
                                                        'COMUNE_RESIDENZA': x[1][0][0]['COMUNE_RESIDENZA'],
                                                        'ASL_APPARTENENZA': x[1][0][0]['ASL_APPARTENENZA'],
                                                        'DATA PRIMA CONSEGNA': x[1][0][0]['DATA PRIMA CONSEGNA'],
                                                        'DATA ULTIMA CONSEGNA': x[1][0][0]['DATA ULTIMA CONSEGNA'],
                                                        'MEDIA GIORNI': x[1][0][0]['MEDIA GIORNI'],
                                                        'NUMERO CONSEGNE TOTALI': x[1][0][0]['NUMERO CONSEGNE TOTALI'],
                                                        'NUMERO RITARDI': x[1][0][0]['NUMERO RITARDI'],
                                                        'ADERENZA': (x[1][0][1]/((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1]))*100
                                                        if (x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1] != 0 else "0000",
                                                        'NUMERATORE ADERENZA(GIORNI DI TERAPIA)': x[1][0][1],
                                                        'DENOMINATORE ADERENZA(NUM GIORNI TRA PRIMA E ULTIMA CONSEGNA)': ((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1])
                                                })


final_rdd_aimovig140 = rdd_join_aimovig140.map(lambda x: {
                                                        'PROD': '1470395',
                                                        'PROD_DESC': 'AIMOVIG*1PEN 140MG 1ML',
                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                        'SESSO': x[1][0][0]['SESSO'],
                                                        'DT_NAS': x[1][0][0]['DT_NAS'],
                                                        'COMUNE NASCITA': x[1][0][0]['COMUNE NASCITA'],
                                                        'COMUNE_RESIDENZA': x[1][0][0]['COMUNE_RESIDENZA'],
                                                        'ASL_APPARTENENZA': x[1][0][0]['ASL_APPARTENENZA'],
                                                        'DATA PRIMA CONSEGNA': x[1][0][0]['DATA PRIMA CONSEGNA'],
                                                        'DATA ULTIMA CONSEGNA': x[1][0][0]['DATA ULTIMA CONSEGNA'],
                                                        'MEDIA GIORNI': x[1][0][0]['MEDIA GIORNI'],
                                                        'NUMERO CONSEGNE TOTALI': x[1][0][0]['NUMERO CONSEGNE TOTALI'],
                                                        'NUMERO RITARDI': x[1][0][0]['NUMERO RITARDI'],
                                                        'ADERENZA': (x[1][0][1]/((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1]))*100
                                                        if (x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1] != 0 else "0000",
                                                        'NUMERATORE ADERENZA(GIORNI DI TERAPIA)': x[1][0][1],
                                                        'DENOMINATORE ADERENZA(NUM GIORNI TRA PRIMA E ULTIMA CONSEGNA)': ((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1])
                                                })



final_rdd_emgality = rdd_join_emgality.map(lambda x: {
                                                        'PROD': '1480002',
                                                        'PROD_DESC': 'EMGALITY*SC 1PEN 120MG 1ML',
                                                        'CODICE PAZIENTE UNIVOCO': x[0],
                                                        'SESSO': x[1][0][0]['SESSO'],
                                                        'DT_NAS': x[1][0][0]['DT_NAS'],
                                                        'COMUNE NASCITA': x[1][0][0]['COMUNE NASCITA'],
                                                        'COMUNE_RESIDENZA': x[1][0][0]['COMUNE_RESIDENZA'],
                                                        'ASL_APPARTENENZA': x[1][0][0]['ASL_APPARTENENZA'],
                                                        'DATA PRIMA CONSEGNA': x[1][0][0]['DATA PRIMA CONSEGNA'],
                                                        'DATA ULTIMA CONSEGNA': x[1][0][0]['DATA ULTIMA CONSEGNA'],
                                                        'MEDIA GIORNI': x[1][0][0]['MEDIA GIORNI'],
                                                        'NUMERO CONSEGNE TOTALI': x[1][0][0]['NUMERO CONSEGNE TOTALI'],
                                                        'NUMERO RITARDI': x[1][0][0]['NUMERO RITARDI'],
                                                        'ADERENZA': (x[1][0][1]/((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1]))*100
                                                        if (x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1] != 0 else "0000",
                                                        'NUMERATORE ADERENZA(GIORNI DI TERAPIA)': x[1][0][1],
                                                        'DENOMINATORE ADERENZA(NUM GIORNI TRA PRIMA E ULTIMA CONSEGNA)': ((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1])
                                                })


final_rdd_ajovy = rdd_join_ajovy.map(lambda x: {
                                                  'PROD': '1487505',
                                                  'PROD_DESC': 'AJOVY*SC 1SIR 1,5ML 225MG',
                                                  'CODICE PAZIENTE UNIVOCO': x[0],
                                                  'SESSO': x[1][0][0]['SESSO'],
                                                  'DT_NAS': x[1][0][0]['DT_NAS'],
                                                  'COMUNE NASCITA': x[1][0][0]['COMUNE NASCITA'],
                                                  'COMUNE_RESIDENZA': x[1][0][0]['COMUNE_RESIDENZA'],
                                                  'ASL_APPARTENENZA': x[1][0][0]['ASL_APPARTENENZA'],
                                                  'DATA PRIMA CONSEGNA': x[1][0][0]['DATA PRIMA CONSEGNA'],
                                                  'DATA ULTIMA CONSEGNA': x[1][0][0]['DATA ULTIMA CONSEGNA'],
                                                  'MEDIA GIORNI': x[1][0][0]['MEDIA GIORNI'],
                                                  'NUMERO CONSEGNE TOTALI': x[1][0][0]['NUMERO CONSEGNE TOTALI'],
                                                  'NUMERO RITARDI': x[1][0][0]['NUMERO RITARDI'],
                                                  'ADERENZA': (x[1][0][1]/((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1]))*100
                                                  if (x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1] != 0 else "0000",
                                                  'NUMERATORE ADERENZA(GIORNI DI TERAPIA)': x[1][0][1],
                                                  'DENOMINATORE ADERENZA(NUM GIORNI TRA PRIMA E ULTIMA CONSEGNA)': ((x[1][0][0]['DATA ULTIMA CONSEGNA']-x[1][0][0]['DATA PRIMA CONSEGNA']).days + x[1][1])
                                                })



final_rdd_with_all_data = final_rdd_aimovig70.union(final_rdd_aimovig140).union(final_rdd_emgality).union(final_rdd_ajovy)

# inserimento nei dataframe di tutte le informazioni raccolte
df_aimovig70_aderenza = pd.DataFrame(final_rdd_aimovig70.collect())
df_aimovig140_aderenza = pd.DataFrame(final_rdd_aimovig140.collect())
df_emgality120_aderenza = pd.DataFrame(final_rdd_emgality.collect())
df_ajovy_aderenza = pd.DataFrame(final_rdd_ajovy.collect())
df_with_all_data = pd.DataFrame(final_rdd_with_all_data.collect())

# cambio del formato data
df_aimovig70_aderenza['DATA PRIMA CONSEGNA'] = df_aimovig70_aderenza['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_aimovig70_aderenza['DATA ULTIMA CONSEGNA'] = df_aimovig70_aderenza['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_aimovig140_aderenza['DATA PRIMA CONSEGNA'] = df_aimovig140_aderenza['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_aimovig140_aderenza['DATA ULTIMA CONSEGNA'] = df_aimovig140_aderenza['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_emgality120_aderenza['DATA PRIMA CONSEGNA'] = df_emgality120_aderenza['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_emgality120_aderenza['DATA ULTIMA CONSEGNA'] = df_emgality120_aderenza['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_ajovy_aderenza['DATA PRIMA CONSEGNA'] = df_ajovy_aderenza['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_ajovy_aderenza['DATA ULTIMA CONSEGNA'] = df_ajovy_aderenza['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

df_with_all_data['DATA PRIMA CONSEGNA'] = df_with_all_data['DATA PRIMA CONSEGNA'].dt.strftime('%d/%m/%Y')
df_with_all_data['DATA ULTIMA CONSEGNA'] = df_with_all_data['DATA ULTIMA CONSEGNA'].dt.strftime('%d/%m/%Y')

# arrotondamento cifre decimali aderenza
df_aimovig70_aderenza['ADERENZA'] = df_aimovig70_aderenza['ADERENZA'].map(lambda x: round(float(x), 2) if x != '0000' else '0000')
df_aimovig140_aderenza['ADERENZA'] = df_aimovig140_aderenza['ADERENZA'].map(lambda x: round(float(x), 2) if x != '0000' else '0000')
df_emgality120_aderenza['ADERENZA'] = df_emgality120_aderenza['ADERENZA'].map(lambda x: round(float(x), 2) if x != '0000' else '0000')
df_ajovy_aderenza['ADERENZA'] = df_ajovy_aderenza['ADERENZA'].map(lambda x: round(float(x), 2) if x != '0000' else '0000')
df_with_all_data['ADERENZA'] = df_with_all_data['ADERENZA'].map(lambda x: round(float(x), 2) if x != '0000' else '0000')

# nomi dei file xlsx finali
names_file_excel = ["dati_con_aderenza_aimovig70_046925018.xlsx", "dati_con_aderenza_aimovig140_046925044.xlsx", "dati_con_aderenza_emgality120_047424015.xlsx", "dati_con_aderenza_ajovy_047791013.xlsx", "dati_con_aderenza.xlsx"]

# conversione dataframe in file xlsx
df_aimovig70_aderenza.to_excel(names_file_excel[0], index=False)
df_aimovig140_aderenza.to_excel(names_file_excel[1], index=False)
df_emgality120_aderenza.to_excel(names_file_excel[2], index=False)
df_ajovy_aderenza.to_excel(names_file_excel[3], index=False)
df_with_all_data.to_excel(names_file_excel[4], index=False)