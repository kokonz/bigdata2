# Project Big Data 2 (Kafka + Spark)

## Big Data A

## Kelompok 4

| Nama                      | NRP        |
| ------------------------- | ---------- |
| Hazwan Adhikara Nasution  | 5027231017 |
| Rafael Gunawan            | 5027231019 |
| Mochamad Fadhil Saifullah | 5027231068 |

### Dataset : https://www.kaggle.com/datasets/amitanshjoshi/spotify-1million-tracks

## Langkah Pengerjaan

1. Install Kafka + Spark.
2. Jalankan server Kafka di <strong>Terminal 1<strong>. <br>
   `bin/kafka-server-start.sh config/server.properties` <br> <br>
   ![image](https://github.com/user-attachments/assets/d7ce4ac6-59d6-4d1d-81d3-40b95902b014)
   <br>

3. Buka <strong>Terminal Baru<strong> dan jalankan producer. <br>
   `python producer_spotify.py` <br>
   ![image](./img/producer.png) <br>

   #### Producer akan mengirim data spotify ke server kafka dengan jeda tiap datanya antara 0.0001-0.001 detik.

   <br>

4. Setelah itu, jalankan consumer di terminal yang sama. <br>
   `python consumer_spotify.py` <br> <br>
   ![image](./img/consumer.png)<br>

   #### Menerima data dari server kafka dan membagi data tersebut menjadi 3 file dataset dalam format CSV (batch\_\*.csv).

   <br>

5. Selanjutnya, jalankan spark di terminal yang sama. <br>
   `spark-submit train_spotify.py` <br> <br>
   ![image](./img/spark%20train.png)

   #### Spark akan membaca batch data yang dihasilkan oleh consumer dan masing-masing batch data tersebut akan di train menggunakan 3 model yang berbeda (rekomendasi, klasifikasi, dan clustering).

   <br> <br> <br>

6. Kemudian, jalankan kode api di terminal yang sama. <br>
   `python api_spotify.py` <br> <br>
   ![image](./img/api.png)

   #### Kode API ini akan memuat model yang telah ditrain tadi dan akan menghasilkan 3 endpoint yang berbeda (/recommend, /predict-genre, dan /cluster-song).

   <br>

7. Terakhir, lakukan testing pada 3 endpoint tersebut. <br>
   a). /recommend
   `curl -X POST http://localhost:5000/recommend \` <br> <br>
   ![image](./img/recommend.png)

   b). /predict-genre
   `curl -X POST http://localhost:5000/predict-genre \` <br> <br>
   ![image](./img/predict%20genre.png) <br>

   c). /cluster-song
   `curl -X POST http://localhost:5000/predict-genre \` <br>
   ![image](./img/cluster.png)
